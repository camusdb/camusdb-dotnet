
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;

namespace CamusDB.Client.Transport;

/// <summary>
/// A <see cref="CamusRowSource"/> that pulls rows from the <c>/execute-sql-query-stream</c> endpoint's
/// newline-delimited JSON body (<c>application/x-ndjson</c>) one line at a time, so a large result never
/// materializes client-side. The wire framing is:
///
/// <code>
/// {"status":"ok","columns":[{"name":"id","type":1}, ...]}   ← header (schema), read once at construction
/// ["6849f3…", 42, true]                                     ← positional row array
/// ["6849f4…", 43, false]
/// {"status":"ok","total":2,"causalToken":{…},"serverTimeMs":3.1}   ← terminal trailer
/// </code>
///
/// Lines are distinguished by their first JSON token: objects are the header / trailer, arrays are rows.
/// Row cells use the identical compact-raw positional encoding as the buffered endpoint, so decoding is
/// shared through <see cref="CamusResultSet.DecodeRow"/>.
///
/// <para><b>In-band failure:</b> because the 200 header (and possibly rows) can already be on the wire
/// before an autocommit transaction commits, a conflict that surfaces mid-stream cannot change the HTTP
/// status; the server reports it as a <c>{"status":"failed",…}</c> trailer. This source surfaces that as a
/// <see cref="CamusException"/> thrown from the <see cref="Read"/> / <see cref="ReadAsync"/> that reaches
/// the trailer — exactly where a buffered call would have thrown. Errors <i>before</i> the first line keep
/// their normal HTTP error body and are translated by the transport, never reaching this type.</para>
/// </summary>
internal sealed class NdjsonStreamRowSource : CamusRowSource
{
    /// <summary>MIME type of the streaming response body; also sent as the request's <c>Accept</c>.</summary>
    public const string ContentType = "application/x-ndjson";

    // The live HTTP response (IFlurlResponse) whose body <see cref="stream"/> reads from; held only to be
    // disposed with the source. Null when the source is driven from a standalone stream (tests).
    private readonly IDisposable? responseHandle;
    private readonly Stream stream;
    private readonly StreamReader reader;

    private readonly string[] names;
    private readonly ColumnType[] types;

    // The header is read at construction; to answer HasRows up front we also prefetch the first row, so a
    // pending row (or the knowledge that there is none) is held before the first Read.
    private ColumnValue[]? pending;
    private bool hasPending;

    private ColumnValue[]? current;
    private bool finished;

    private NdjsonStreamRowSource(
        IDisposable? responseHandle, Stream stream, StreamReader reader, string[] names, ColumnType[] types)
    {
        this.responseHandle = responseHandle;
        this.stream = stream;
        this.reader = reader;
        this.names = names;
        this.types = types;
    }

    public override string[] ColumnNames => names;

    public override ColumnType[]? ColumnTypes => types;

    public override bool HasRows => hasPending && pending is not null;

    /// <summary>
    /// Reads the schema header and prefetches the first row (so <see cref="HasRows"/> is answerable),
    /// leaving the source positioned before the first row. A missing / malformed header is a protocol
    /// violation and surfaces as a <see cref="CamusException"/>.
    /// </summary>
    public static Task<NdjsonStreamRowSource> CreateAsync(
        IDisposable? responseHandle, Stream stream, CancellationToken cancellationToken)
    {
        StreamReader reader = new(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: false);
        return CreateAsync(responseHandle, stream, reader, cancellationToken);
    }

    private static async Task<NdjsonStreamRowSource> CreateAsync(
        IDisposable? responseHandle, Stream stream, StreamReader reader, CancellationToken cancellationToken)
    {
        string? headerLine = await ReadNonEmptyLineAsync(reader, cancellationToken).ConfigureAwait(false);
        if (headerLine is null)
            throw new CamusException("CADB0000", "Streaming query response ended before the schema header.");

        (string[] names, ColumnType[] types) = ParseHeader(headerLine);

        NdjsonStreamRowSource source = new(responseHandle, stream, reader, names, types);
        source.pending = await source.ReadNextRowAsync(cancellationToken).ConfigureAwait(false);
        source.hasPending = true;
        return source;
    }

    public override bool Read()
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

        current = ReadNextRow(ReadNonEmptyLine(reader));
        return current is not null;
    }

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

    public override ColumnValue GetCell(int ordinal)
    {
        if (current is null)
            throw new InvalidOperationException("No current row is available.");

        return current[ordinal];
    }

    private async Task<ColumnValue[]?> ReadNextRowAsync(CancellationToken cancellationToken)
        => ReadNextRow(await ReadNonEmptyLineAsync(reader, cancellationToken).ConfigureAwait(false));

    /// <summary>
    /// Interprets one NDJSON line: an array is a row (decoded and returned); an object is the terminal
    /// trailer (marks the stream finished, and throws if it reports an in-band failure); a null line is a
    /// truncated stream (treated as end). Returns null at end of stream.
    /// </summary>
    private ColumnValue[]? ReadNextRow(string? line)
    {
        if (line is null)
        {
            finished = true;
            return null;
        }

        using JsonDocument doc = JsonDocument.Parse(line);
        JsonElement root = doc.RootElement;

        if (root.ValueKind == JsonValueKind.Array)
            return CamusResultSet.DecodeRow(root, types);

        // Object => trailer. This is the terminal line; a failed status is an in-band error.
        finished = true;
        ThrowIfFailed(root);
        return null;
    }

    private static (string[] Names, ColumnType[] Types) ParseHeader(string headerLine)
    {
        using JsonDocument doc = JsonDocument.Parse(headerLine);
        JsonElement root = doc.RootElement;

        if (root.ValueKind != JsonValueKind.Object)
            throw new CamusException("CADB0000", "Streaming query response did not start with a schema header.");

        // A header can, in principle, already carry a failure (defensive — the server emits the header
        // before pulling rows, so this is rare).
        ThrowIfFailed(root);

        if (!root.TryGetProperty("columns", out JsonElement columns) || columns.ValueKind != JsonValueKind.Array)
            return ([], []);

        int count = columns.GetArrayLength();
        string[] names = new string[count];
        ColumnType[] types = new ColumnType[count];

        int i = 0;
        foreach (JsonElement column in columns.EnumerateArray())
        {
            names[i] = column.TryGetProperty("name", out JsonElement name) ? name.GetString() ?? "" : "";
            types[i] = column.TryGetProperty("type", out JsonElement type) && type.TryGetInt32(out int t)
                ? (ColumnType)t
                : ColumnType.Null;
            i++;
        }

        return (names, types);
    }

    private static void ThrowIfFailed(JsonElement meta)
    {
        if (!meta.TryGetProperty("status", out JsonElement status) ||
            !string.Equals(status.GetString(), "failed", StringComparison.Ordinal))
            return;

        string code = meta.TryGetProperty("code", out JsonElement c) ? c.GetString() ?? "CADB0000" : "CADB0000";
        string message = meta.TryGetProperty("message", out JsonElement m) ? m.GetString() ?? "" : "";
        throw new CamusException(code, message);
    }

    // The server writes exactly one record per line (value + '\n'), but skip any blank line defensively so
    // stray framing never surfaces as a spurious end-of-stream.
    private static async Task<string?> ReadNonEmptyLineAsync(StreamReader reader, CancellationToken cancellationToken)
    {
        while (true)
        {
            string? line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line is null)
                return null;
            if (line.Length != 0)
                return line;
        }
    }

    private static string? ReadNonEmptyLine(StreamReader reader)
    {
        while (true)
        {
            string? line = reader.ReadLine();
            if (line is null)
                return null;
            if (line.Length != 0)
                return line;
        }
    }

    public override void Dispose()
    {
        reader.Dispose();
        stream.Dispose();
        responseHandle?.Dispose();
    }

    public override async ValueTask DisposeAsync()
    {
        reader.Dispose();
        await stream.DisposeAsync().ConfigureAwait(false);
        responseHandle?.Dispose();
    }
}
