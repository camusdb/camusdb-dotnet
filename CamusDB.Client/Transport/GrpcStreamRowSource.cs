/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Grpc;
using Grpc.Core;

namespace CamusDB.Client.Transport;

/// <summary>
/// A <see cref="CamusRowSource"/> that pulls rows from the server-streaming <c>CamusSql.ExecuteQuery</c>
/// RPC one message at a time, so a large result never materializes client-side. The wire order is:
///
/// <code>
/// ResultSchema                      ← exactly one, read at construction, even for an empty result
/// ResultRow, ResultRow, …           ← one message per row, as the server's cursor produces them
/// CacheMetadata / RoutingAdvice     ← optional, after the last row; not exposed by a streaming reader
/// trailers                          ← the causal token of an autocommit statement, or the error
/// </code>
///
/// <para>The batched path (<c>BatchExecute</c>) cannot do this: the batcher completes an operation only
/// once every row has arrived, so a streamed read through it held the whole table twice — as protobuf
/// rows, then as the decoded result set. A unary-style call of its own also gives HTTP/2 flow control
/// back to the reader: when the caller stops reading, the server's writes wait instead of queueing.</para>
///
/// <para><b>Failure:</b> an autocommit statement commits after its last row, so a conflict can surface
/// after rows were already read. The server reports it as the call's status, which arrives as an
/// <see cref="RpcException"/> from the read that reaches the end; it is thrown from
/// <see cref="ReadAsync"/> as the <see cref="CamusException"/> a buffered call would have thrown.</para>
///
/// <para><b>Timeout:</b> each wait for the next message — the schema, the first row, every row after
/// it — is bounded by the command timeout on its own, rather than the call as a whole. A dump of a
/// large table can take longer than any sensible command timeout while the server is answering
/// steadily; what the timeout has to catch is a server that has stopped answering. The time the caller
/// spends between reads is not counted.</para>
/// </summary>
internal sealed class GrpcStreamRowSource : CamusRowSource
{
    private readonly AsyncServerStreamingCall<QueryStreamMessage> call;

    // Cancels the call when one wait for a message runs past the timeout. Null when no timeout applies.
    private readonly CancellationTokenSource? idle;
    private readonly TimeSpan idleTimeout;

    private readonly Func<RpcException, CamusException> translate;

    // Handed the call's trailers once the stream has ended cleanly, for the causal token.
    private readonly Action<global::Grpc.Core.Metadata>? observeTrailers;

    private readonly string[] names;
    private readonly ColumnType[] types;

    // The schema is read at construction; to answer HasRows up front the first row is prefetched too.
    private ColumnValue[]? pending;
    private bool hasPending;

    private ColumnValue[]? current;
    private bool finished;

    // Two arrays reused across rows, as in NdjsonStreamRowSource: a decode always fills 'scratch',
    // which is never the array the caller is reading through.
    private ColumnValue[] scratch;
    private ColumnValue[] spare;

    private GrpcStreamRowSource(
        AsyncServerStreamingCall<QueryStreamMessage> call,
        CancellationTokenSource? idle,
        TimeSpan idleTimeout,
        Func<RpcException, CamusException> translate,
        Action<global::Grpc.Core.Metadata>? observeTrailers,
        string[] names,
        ColumnType[] types)
    {
        this.call = call;
        this.idle = idle;
        this.idleTimeout = idleTimeout;
        this.translate = translate;
        this.observeTrailers = observeTrailers;
        this.names = names;
        this.types = types;
        scratch = new ColumnValue[types.Length];
        spare = new ColumnValue[types.Length];
    }

    public override string[] ColumnNames => names;

    public override ColumnType[]? ColumnTypes => types;

    public override bool HasRows => hasPending && pending is not null;

    /// <summary>
    /// Reads the schema and prefetches the first row, leaving the source positioned before the first
    /// row. Takes ownership of <paramref name="call"/> and <paramref name="idle"/>: both are disposed
    /// here if this fails, and with the source otherwise. <paramref name="idle"/> must be the source
    /// whose token the call was started with.
    /// </summary>
    public static async Task<GrpcStreamRowSource> CreateAsync(
        AsyncServerStreamingCall<QueryStreamMessage> call,
        CancellationTokenSource? idle,
        int timeoutSeconds,
        Func<RpcException, CamusException> translate,
        Action<global::Grpc.Core.Metadata>? observeTrailers,
        CancellationToken cancellationToken)
    {
        GrpcStreamRowSource? source = null;
        try
        {
            TimeSpan idleTimeout = TimeSpan.FromSeconds(Math.Max(0, timeoutSeconds));

            if (!await MoveNextAsync(call, idle, idleTimeout, translate, cancellationToken).ConfigureAwait(false))
                throw new CamusException("CADB0000", "Streaming query response ended before the schema.");

            QueryStreamMessage first = call.ResponseStream.Current;
            if (first.PayloadCase != QueryStreamMessage.PayloadOneofCase.Schema)
                throw new CamusException("CADB0000", "Streaming query response did not start with a schema.");

            (string[] names, ColumnType[] types) = ReadSchema(first.Schema);

            source = new(call, idle, idleTimeout, translate, observeTrailers, names, types);
            source.pending = await source.ReadNextRowAsync(cancellationToken).ConfigureAwait(false);
            source.hasPending = true;
            return source;
        }
        catch
        {
            if (source is not null)
                source.Dispose();
            else
            {
                call.Dispose();
                idle?.Dispose();
            }

            throw;
        }
    }

    // Sync-over-async: gRPC exposes only an asynchronous read. The buffered path did the same to give a
    // synchronous ExecuteReader, so a synchronous caller is no worse off than before.
    public override bool Read() => ReadAsync(CancellationToken.None).AsTask().GetAwaiter().GetResult();

    public override async ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (hasPending)
        {
            current = pending;
            pending = null;
            hasPending = false;
            return current is not null;
        }

        if (finished)
        {
            current = null;
            return false;
        }

        current = await ReadNextRowAsync(cancellationToken).ConfigureAwait(false);
        return current is not null;
    }

    public override ref readonly ColumnValue GetCell(int ordinal)
    {
        if (current is null)
            throw new InvalidOperationException("No current row is available.");

        return ref current[ordinal];
    }

    /// <summary>
    /// Reads up to the next row and decodes it, skipping the messages that follow the last row. Returns
    /// null at the end of the stream, after the trailers have been observed.
    /// </summary>
    private async ValueTask<ColumnValue[]?> ReadNextRowAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await MoveNextAsync(call, idle, idleTimeout, translate, cancellationToken).ConfigureAwait(false))
            {
                QueryStreamMessage message = call.ResponseStream.Current;

                switch (message.PayloadCase)
                {
                    case QueryStreamMessage.PayloadOneofCase.Row:
                        return Decode(message.Row);

                    case QueryStreamMessage.PayloadOneofCase.Schema:
                        throw new CamusException("CADB0000", "Streaming query response sent a second schema.");

                    // The cache verdict and the routing advice: the streaming reader exposes neither, on
                    // either transport (see CamusCommand.ExecuteStreamReaderAsync).
                    default:
                        continue;
                }
            }
        }
        catch
        {
            finished = true;
            throw;
        }

        finished = true;
        observeTrailers?.Invoke(call.GetTrailers());
        return null;
    }

    private ColumnValue[] Decode(ResultRow row)
    {
        Google.Protobuf.Collections.RepeatedField<Value> values = row.Values;
        int valueCount = values.Count;

        for (int c = 0; c < scratch.Length; c++)
            scratch[c] = c < valueCount ? GrpcValueCodec.Decode(values[c]) : ColumnValue.Null;

        ColumnValue[] decoded = scratch;
        (scratch, spare) = (spare, scratch);
        return decoded;
    }

    /// <summary>
    /// Advances the stream by one message, with <paramref name="idle"/> armed only while the read is
    /// actually waiting. A message already buffered completes synchronously, and most rows of a large
    /// result do, so they pay for no timer at all.
    /// </summary>
    private static async ValueTask<bool> MoveNextAsync(
        AsyncServerStreamingCall<QueryStreamMessage> call,
        CancellationTokenSource? idle,
        TimeSpan idleTimeout,
        Func<RpcException, CamusException> translate,
        CancellationToken cancellationToken)
    {
        try
        {
            Task<bool> next = call.ResponseStream.MoveNext(cancellationToken);

            if (next.IsCompleted || idle is null)
                return await next.ConfigureAwait(false);

            idle.CancelAfter(idleTimeout);
            try
            {
                return await next.ConfigureAwait(false);
            }
            finally
            {
                idle.CancelAfter(Timeout.InfiniteTimeSpan);
            }
        }
        catch (RpcException ex) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException("The streaming query was cancelled.", ex, cancellationToken);
        }
        catch (RpcException) when (idle is { IsCancellationRequested: true })
        {
            throw translate(new RpcException(new Status(
                StatusCode.DeadlineExceeded,
                $"No response arrived within {idleTimeout.TotalSeconds:0} s while streaming the query.")));
        }
        catch (RpcException ex)
        {
            throw translate(ex);
        }
    }

    private static (string[] Names, ColumnType[] Types) ReadSchema(ResultSchema schema)
    {
        int columnCount = schema.Columns.Count;
        string[] names = new string[columnCount];
        ColumnType[] types = new ColumnType[columnCount];

        for (int i = 0; i < columnCount; i++)
        {
            ColumnSchema column = schema.Columns[i];
            names[i] = column.Name;
            types[i] = GrpcValueCodec.ToClientColumnType(column.Type);
        }

        return (names, types);
    }

    // Disposing a call that has not finished cancels it, which is how a reader closed early tells the
    // server to stop: an autocommit read is rolled back rather than left holding its snapshot.
    public override void Dispose()
    {
        call.Dispose();
        idle?.Dispose();
    }
}
