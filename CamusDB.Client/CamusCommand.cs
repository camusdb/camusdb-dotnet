
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using CamusDB.Client.Transport;
using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client;

/// <summary>
/// Represents a SQL query or command to execute against a Camus database.
/// 
/// If the command is a SQL query, then <see cref="CamusCommand.CommandText"/>
/// contains the entire SQL statement. Use <see cref="CamusCommand.ExecuteReaderAsync()"/>  to obtain results.
///
/// If the command is an update, insert or delete command, then <see cref="CamusCommand.CommandText"/>
/// is simply "[operation] [camus_table]" such as "UPDATE MYTABLE" with the parameter
/// collection containing <see cref="CamusParameter"/> instances whose name matches a column
/// in the target table. Use <see cref="ExecuteNonQueryAsync"/> to execute the command.
///
/// The command may also be a DDL statement such as CREATE TABLE. Use <see cref="ExecuteNonQueryAsync"/>
/// to execute a DDL statement.
/// </summary>
public class CamusCommand : DbCommand, ICloneable
{
    protected readonly CamusConnectionStringBuilder builder;

    protected CamusTransaction? transaction;

    private CamusConnection? connection;

    private bool designTimeVisible;

    private UpdateRowSource updatedRowSource;

    public CamusCommand(string source, CamusConnectionStringBuilder builder, CamusConnection? connection = null)
    {
        this.builder = builder;
        this.connection = connection;
        CommandText = source;
        updatedRowSource = UpdateRowSource.None;
        CommandTimeout = builder.CommandTimeout;
    }

    /// <summary>
    /// The parameters of the SQL statement or command.
    /// </summary>
    public new CamusParameterCollection Parameters { get; } = new CamusParameterCollection();

    /// <summary>
    /// Query result cache metadata reported by the server for the most recent reader query executed
    /// through this command, or <see langword="null"/> if that query carried no <c>{cache=…}</c> hint
    /// (or no reader query has run yet). Also available on <see cref="CamusDataReader.CacheMetadata"/>.
    /// </summary>
    public CamusCacheMetadata? LastCacheMetadata { get; private set; }

    [AllowNull]
    public override string CommandText { get; set; } = "";

    public override int CommandTimeout { get; set; } = 10;

    /// <summary>
    /// Always <see cref="System.Data.CommandType.Text"/> in practice — CamusDB has no stored procedures
    /// or direct table access — but it defaults to it explicitly rather than to <c>default(CommandType)</c>,
    /// which is not a valid value at all. Callers that never touch this property (EF Core among them)
    /// would otherwise present a command whose type is zero, and anything reading it to decide what a
    /// command is would have to special-case that.
    /// </summary>
    public override CommandType CommandType { get; set; } = CommandType.Text;

    public override bool DesignTimeVisible { get => designTimeVisible; set => designTimeVisible = value; }

    public override UpdateRowSource UpdatedRowSource { get => updatedRowSource; set => updatedRowSource = value; }

    protected override DbConnection? DbConnection
    {
        get => connection;
        set
        {
            if (value is not null and not CamusConnection)
                throw new ArgumentException("Value must be a CamusConnection.", nameof(value));

            connection = (CamusConnection?)value;
        }
    }

    protected override DbParameterCollection DbParameterCollection => Parameters;

    protected override DbTransaction? DbTransaction { get => transaction; set => transaction = (CamusTransaction?) value; }

    /// <summary>
    /// The effective concurrency options for an autocommit statement (no explicit <see cref="Transaction"/>):
    /// the connection's resolved defaults, or the connection-string defaults when this command has no
    /// connection. Applied to the writable autocommit paths; the read-only query path has no locking mode.
    /// </summary>
    private CamusTransactionOptions ResolveAutocommitOptions()
        => connection is not null ? connection.ResolveTransactionOptions(null) : builder.DefaultTransactionOptions;

    public override void Cancel()
    {
        // CamusDB uses HTTP requests, so cancellation is cooperative via CancellationToken.
    }

    public object Clone()
    {
        CamusCommand clone = this switch
        {
            CamusInsertCommand => new CamusInsertCommand(CommandText, builder, connection),
            CamusPingCommand => new CamusPingCommand(CommandText, builder, connection),
            _ => new CamusCommand(CommandText, builder, connection)
        };

        clone.CommandTimeout = CommandTimeout;
        clone.CommandType = CommandType;
        clone.DesignTimeVisible = DesignTimeVisible;
        clone.UpdatedRowSource = UpdatedRowSource;
        clone.transaction = transaction;

        foreach (CamusParameter parameter in Parameters)
            clone.Parameters.Add((CamusParameter)parameter.Clone());

        return clone;
    }

    protected string GetRequestTarget() => CommandText;

    protected string GetEndpoint() => transaction?.Endpoint ?? builder.GetEndpoint();

    private static readonly string[] DdlPrefixes =
    [
        "CREATE TABLE",
        "DROP TABLE",
        "ALTER TABLE",
        "CREATE UNIQUE INDEX",
        "CREATE INDEX",
        "DROP INDEX",
    ];

    private static readonly string[] DmlPrefixes =
    [
        "INSERT",
        "UPDATE",
        "DELETE",
    ];

    private static bool IsDdlStatement(string sql) => StartsWithAny(sql, DdlPrefixes);

    private static bool IsDmlStatement(string sql) => StartsWithAny(sql, DmlPrefixes);

    private static bool StartsWithAny(string sql, string[] prefixes)
    {
        ReadOnlySpan<char> trimmed = sql.AsSpan().TrimStart();
        foreach (string prefix in prefixes)
        {
            if (trimmed.Length >= prefix.Length &&
                trimmed[..prefix.Length].Equals(prefix.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    /// <summary>Bound parameters as the transport-neutral dictionary, or null when the command has none —
    /// the common case for EF-generated reads, which would otherwise allocate an empty dictionary each.</summary>
    protected Dictionary<string, ColumnValue>? GetCommandParameters()
    {
        if (Parameters.Count == 0)
            return null;

        Dictionary<string, ColumnValue> commandParameters = new(Parameters.Count);

        foreach (CamusParameter parameter in Parameters)
        {
            if (string.IsNullOrEmpty(parameter.ParameterName))
                throw new CamusException("CADB0400", "Parameter name cannot be null or empty");

            commandParameters.Add(
                parameter.ParameterName,
                BuildColumnValue(parameter.ParameterName, parameter.ColumnType, parameter.Value, parameter.ArrayElementType));
        }

        return commandParameters;
    }

    private static ColumnValue BuildColumnValue(string name, ColumnType columnType, object? value, ColumnType arrayElementType)
    {
        if (value is null or DBNull || columnType == ColumnType.Null)
            return new() { Type = ColumnType.Null };

        switch (columnType)
        {
            case ColumnType.Id or ColumnType.String when value is string s:
                return new() { Type = columnType, StrValue = s };

            case ColumnType.Id or ColumnType.String when value is Guid g:
                return new() { Type = columnType, StrValue = g.ToString() };

            case ColumnType.Id when value is CamusObjectIdValue:
                return new() { Type = columnType, StrValue = value.ToString() };

            // The server accepts a UUID parameter as its canonical string form and re-splits it into
            // the big-endian halves on its side (see ColumnValue's JsonConstructor). The raw halves are
            // carried too so the gRPC codec serializes them directly instead of re-parsing the string.
            case ColumnType.Uuid when value is Guid gu:
                return UuidColumnValue(gu);

            case ColumnType.Uuid when value is string us:
                return new() { Type = columnType, StrValue = us };

            case ColumnType.Integer64 when value is IConvertible ci:
                return new() { Type = columnType, LongValue = ci.ToInt64(CultureInfo.InvariantCulture) };

            case ColumnType.Float64 or ColumnType.Float32 when value is IConvertible cf:
                return new() { Type = columnType, FloatValue = cf.ToDouble(CultureInfo.InvariantCulture) };

            case ColumnType.Bool when value is bool b:
                return new() { Type = columnType, BoolValue = b };

            case ColumnType.Bytes:
                return new() { Type = columnType, BytesValue = ToBytes(name, value) };

            case ColumnType.Date:
                return new() { Type = columnType, LongValue = ToDateTimeUtc(name, value).Date.Ticks };

            case ColumnType.DateTime:
                return new() { Type = columnType, LongValue = ToDateTimeUtc(name, value).Ticks };

            case ColumnType.Array:
                return BuildArrayColumnValue(name, value, arrayElementType);

            case ColumnType.String:
                return new() { Type = ColumnType.String, StrValue = Convert.ToString(value, CultureInfo.InvariantCulture) ?? "" };

            default:
                throw new CamusException("CADB0400", $"Cannot map parameter '{name}' (ColumnType={columnType}, ValueType={value.GetType().Name})");
        }
    }

    private static ColumnValue BuildArrayColumnValue(string name, object value, ColumnType arrayElementType)
    {
        if (value is string || value is not IEnumerable enumerable)
            throw new CamusException("CADB0400", $"Array parameter '{name}' requires an IEnumerable value (got {value.GetType().Name})");

        List<object?> items = [];
        foreach (object? item in enumerable)
            items.Add(item);

        ColumnType elementType = arrayElementType;
        if (elementType == ColumnType.Null)
        {
            foreach (object? item in items)
            {
                if (item is null or DBNull)
                    continue;
                elementType = InferColumnType(item.GetType());
                break;
            }

            if (elementType == ColumnType.Null && items.Count > 0)
                throw new CamusException("CADB0400", $"Cannot infer element type for array parameter '{name}'; set CamusParameter.ArrayElementType explicitly");
        }

        List<ColumnValue> elements = new(items.Count);
        foreach (object? item in items)
        {
            elements.Add(item is null or DBNull
                ? new() { Type = ColumnType.Null }
                : BuildColumnValue(name, elementType, item, ColumnType.Null));
        }

        return new() { Type = ColumnType.Array, ArrayElementType = elementType, ArrayValues = elements };
    }

    private static ColumnValue UuidColumnValue(in Guid guid)
    {
        Span<byte> bytes = stackalloc byte[16];
        guid.TryWriteBytes(bytes, bigEndian: true, out _);

        return new()
        {
            Type = ColumnType.Uuid,
            StrValue = guid.ToString(),
            UuidHigh = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(bytes[..8]),
            LongValue = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(bytes[8..]),
        };
    }

    private static byte[] ToBytes(string name, object value) => value switch
    {
        byte[] bytes => bytes,
        ReadOnlyMemory<byte> rom => rom.ToArray(),
        Memory<byte> mem => mem.ToArray(),
        ArraySegment<byte> seg => seg.ToArray(),
        IEnumerable<byte> seq => [.. seq],
        _ => throw new CamusException("CADB0400", $"Cannot map bytes parameter '{name}' from {value.GetType().Name}")
    };

    private static DateTime ToDateTimeUtc(string name, object value) => value switch
    {
        DateTime dt => dt.Kind switch
        {
            DateTimeKind.Utc => dt,
            DateTimeKind.Local => dt.ToUniversalTime(),
            _ => DateTime.SpecifyKind(dt, DateTimeKind.Utc)
        },
        DateTimeOffset dto => dto.UtcDateTime,
        DateOnly d => d.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc),
        string str => DateTime.Parse(str, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal),
        _ => throw new CamusException("CADB0400", $"Cannot map date/datetime parameter '{name}' from {value.GetType().Name}")
    };

    private static ColumnType InferColumnType(Type type) => type switch
    {
        _ when type == typeof(string) => ColumnType.String,
        _ when type == typeof(bool) => ColumnType.Bool,
        _ when type == typeof(byte) || type == typeof(sbyte)
            || type == typeof(short) || type == typeof(ushort)
            || type == typeof(int) || type == typeof(uint)
            || type == typeof(long) || type == typeof(ulong) => ColumnType.Integer64,
        _ when type == typeof(float) => ColumnType.Float32,
        _ when type == typeof(double) || type == typeof(decimal) => ColumnType.Float64,
        _ when type == typeof(DateTime) || type == typeof(DateTimeOffset) => ColumnType.DateTime,
        _ when type == typeof(DateOnly) => ColumnType.Date,
        _ when type == typeof(Guid) => ColumnType.Uuid,
        _ when type == typeof(CamusObjectIdValue) => ColumnType.Id,
        _ when type == typeof(byte[]) => ColumnType.Bytes,
        _ => ColumnType.Null
    };

    /// <summary>
    /// Sends the command to CamusDB and builds a <see cref="CamusDBDataReader"/>.
    /// </summary>
    /// <returns>An asynchronous <see cref="Task"/> that produces a <see cref="CamusDBDataReader"/>.</returns>
    public new Task<CamusDataReader> ExecuteReaderAsync() =>
        ExecuteReaderAsync(CommandBehavior.Default, CancellationToken.None);

    /// <summary>
    /// Sends the command to CamusDB and builds a <see cref="CamusDBDataReader"/>.
    /// </summary>
    /// <param name="cancellationToken">An optional token for canceling the call.</param>
    /// <returns>An asynchronous <see cref="Task"/> that produces a <see cref="CamusDBDataReader"/>.</returns>
    public new Task<CamusDataReader> ExecuteReaderAsync(CancellationToken cancellationToken) =>
        ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

    /// <summary>
    /// Sends the command to CamusDB and builds a <see cref="CamusDBDataReader"/>.
    /// </summary>
    /// <param name="behavior">Options for statement execution and data retrieval.</param>
    /// <returns>An asynchronous <see cref="Task"/> that produces a <see cref="CamusDBDataReader"/>.</returns>
    public new Task<CamusDataReader> ExecuteReaderAsync(CommandBehavior behavior) =>
        ExecuteReaderAsync(behavior, CancellationToken.None);

    /// <summary>
    /// Sends the command to CamusDB and builds a <see cref="CamusDBDataReader"/>.
    /// </summary>
    /// <param name="behavior">Options for statement execution and data retrieval.</param>
    /// <param name="cancellationToken">An optional token for canceling the call.</param>
    /// <returns>An asynchronous <see cref="Task"/> that produces a <see cref="CamusDBDataReader"/>.</returns>
    public new async Task<CamusDataReader> ExecuteReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
        (CamusDataReader)await ExecuteDbDataReaderAsync(behavior, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Like <see cref="ExecuteReaderAsync()"/>, but pulls rows from the server incrementally over the
    /// streaming (<c>/execute-sql-query-stream</c>) endpoint instead of buffering the whole result set —
    /// so a large <c>SELECT</c> never fully materializes client-side. Rows arrive as the returned reader is
    /// advanced (ideally with <see cref="CamusDataReader.ReadAsync(System.Threading.CancellationToken)"/>);
    /// dispose the reader to release the underlying HTTP response.
    ///
    /// <para>The streaming path forfeits the buffered path's transparent serializable retry: because rows
    /// can reach the client before the autocommit transaction commits, a late conflict is reported while
    /// reading (as a <see cref="CamusException"/>) rather than retried. Use the buffered
    /// <see cref="ExecuteReaderAsync()"/> — or drive an explicit transaction and retry yourself — when you
    /// need that. Streaming applies to queries; a DML statement falls back to the buffered affected-row
    /// reader. On the gRPC transport this currently buffers server-side and replays.</para>
    /// </summary>
    public Task<CamusDataReader> ExecuteStreamReaderAsync() =>
        ExecuteStreamReaderAsync(CancellationToken.None);

    /// <inheritdoc cref="ExecuteStreamReaderAsync()"/>
    public async Task<CamusDataReader> ExecuteStreamReaderAsync(CancellationToken cancellationToken)
    {
        if (IsDmlStatement(CommandText))
            return await ExecuteDmlAsReaderAsync(cancellationToken).ConfigureAwait(false);

        string endpoint = GetEndpoint();

        TransportSqlRequest request = new()
        {
            Endpoint = endpoint,
            Database = builder.Config["Database"],
            Sql = GetRequestTarget(),
            Parameters = GetCommandParameters(),
            TxnIdPT = transaction?.TxnIdPT,
            TxnIdCounter = transaction?.TxnIdCounter,
            StreamSlot = transaction?.StreamSlot,
            TimeoutSeconds = CommandTimeout,
            Prepared = await ShouldPrepareAsync(endpoint, cancellationToken).ConfigureAwait(false),
        };

        CamusRowSource source = await builder.GetTransport().ExecuteQueryStreamAsync(request, cancellationToken).ConfigureAwait(false);

        // The streaming endpoint carries no cache metadata (its trailer has no cache fields).
        LastCacheMetadata = null;

        return new CamusDataReader(source);
    }

    /// <inheritdoc />
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
        ExecuteDbDataReaderAsync(behavior, default).GetAwaiter().GetResult();

    /// <inheritdoc />
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (IsDmlStatement(CommandText))
            return await ExecuteDmlAsReaderAsync(cancellationToken).ConfigureAwait(false);

        string endpoint = GetEndpoint();

        TransportSqlRequest request = new()
        {
            Endpoint = endpoint,
            Database = builder.Config["Database"],
            Sql = GetRequestTarget(),
            Parameters = GetCommandParameters(),
            TxnIdPT = transaction?.TxnIdPT,
            TxnIdCounter = transaction?.TxnIdCounter,
            StreamSlot = transaction?.StreamSlot,
            TimeoutSeconds = CommandTimeout,
            Prepared = await ShouldPrepareAsync(endpoint, cancellationToken).ConfigureAwait(false),
        };

        QueryTransportResult result = await builder.GetTransport().ExecuteQueryAsync(request, cancellationToken).ConfigureAwait(false);

        LastCacheMetadata = result.CacheMetadata;

        return new CamusDataReader(result.ResultSet, LastCacheMetadata);
    }

    private async Task<CamusDataReader> ExecuteDmlAsReaderAsync(CancellationToken cancellationToken)
    {
        int affectedRows = await ExecuteNonQueryCoreAsync(cancellationToken).ConfigureAwait(false);

        return new CamusDataReader(affectedRows);
    }

    /// <summary>
    /// Builds the protocol-neutral request for a DML/non-query statement: joins the explicit
    /// <see cref="Transaction"/> when present, otherwise carries the resolved autocommit concurrency
    /// options for the short transaction the server begins for this statement.
    /// </summary>
    private async ValueTask<TransportSqlRequest> BuildNonQueryTransportRequestAsync(CancellationToken cancellationToken)
    {
        string endpoint = GetEndpoint();

        return new()
        {
            Endpoint = endpoint,
            Database = builder.Config["Database"],
            Sql = GetRequestTarget(),
            Parameters = GetCommandParameters(),
            TxnIdPT = transaction?.TxnIdPT,
            TxnIdCounter = transaction?.TxnIdCounter,
            StreamSlot = transaction?.StreamSlot,
            AutocommitOptions = transaction is null ? ResolveAutocommitOptions() : null,
            TimeoutSeconds = CommandTimeout,
            Prepared = await ShouldPrepareAsync(endpoint, cancellationToken).ConfigureAwait(false),
        };
    }

    private async Task<int> ExecuteNonQueryCoreAsync(CancellationToken cancellationToken)
    {
        TransportSqlRequest request = await BuildNonQueryTransportRequestAsync(cancellationToken).ConfigureAwait(false);

        return await builder.GetTransport().ExecuteNonQueryAsync(request, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes the command and returns the number of rows affected.
    /// This method runs syncrhonously
    /// </summary>
    /// <returns></returns>
    public override int ExecuteNonQuery()
    {
        return ExecuteNonQueryAsync(default).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        if (IsDdlStatement(CommandText))
        {
            await ExecuteDDLAsync(cancellationToken).ConfigureAwait(false);
            return 0;
        }

        return await ExecuteNonQueryCoreAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Executes a DDL command and returns the success status
    /// This method runs syncrhonously
    /// </summary>
    /// <returns></returns>
    public bool ExecuteDDL()
    {
        return ExecuteDDLAsync(default).GetAwaiter().GetResult();
    }

    /// <inheritdoc />
    public Task<bool> ExecuteDDLAsync()
    {
        return ExecuteDDLAsync(default);
    }

    /// <inheritdoc />
    public async Task<bool> ExecuteDDLAsync(CancellationToken cancellationToken)
    {
        TransportSqlRequest request = new()
        {
            Endpoint = GetEndpoint(),
            Database = builder.Config["Database"],
            Sql = GetRequestTarget(),
            TxnIdPT = transaction?.TxnIdPT,
            TxnIdCounter = transaction?.TxnIdCounter,
            StreamSlot = transaction?.StreamSlot,
            AutocommitOptions = transaction is null ? ResolveAutocommitOptions() : null,
            TimeoutSeconds = CommandTimeout,
        };

        return await builder.GetTransport().ExecuteDdlAsync(request, cancellationToken).ConfigureAwait(false);
    }

    public override object? ExecuteScalar()
    {
        using DbDataReader reader = ExecuteDbDataReader(CommandBehavior.SingleRow);

        if (!reader.Read() || reader.FieldCount == 0)
            return null;

        return reader.GetValue(0);
    }

    /// <summary>
    /// Registers <see cref="CommandText"/> with the server so this and every later command running the
    /// same SQL sends only a handle and its parameter values.
    ///
    /// <para>Calling this is optional. The driver prepares a statement on its own once it has seen the
    /// same SQL a few times (see <c>MaxAutoPrepare=</c> / <c>AutoPrepareMinUsages=</c>), which is what
    /// makes Entity Framework Core — which never calls <see cref="Prepare"/> — benefit. Call it to skip
    /// the warm-up for a statement you already know is hot.</para>
    ///
    /// <para>Registration is idempotent per (endpoint, database, SQL) and shared across every connection
    /// built from the same connection string, so preparing twice costs nothing. A statement that cannot
    /// be prepared — DDL, database administration, or a server that has no prepared-statement support —
    /// is remembered as such and simply keeps running inline; that is not an error, and this method does
    /// not throw for it.</para>
    /// </summary>
    public override void Prepare() => PrepareAsync().GetAwaiter().GetResult();

    /// <inheritdoc cref="Prepare"/>
    public override async Task PrepareAsync(CancellationToken cancellationToken = default)
    {
        if (!IsPreparableStatement(CommandText))
            return;

        CamusPreparedStatementPolicy policy = builder.PreparedStatements;
        string database = builder.Config["Database"];
        string endpoint = GetEndpoint();

        if (policy.Pin(database, CommandText, out (string Database, string Sql)? evicted) == PrepareDecision.Register)
            await RegisterAsync(policy, endpoint, database, CommandText, cancellationToken).ConfigureAwait(false);

        Release(endpoint, evicted);
    }

    /// <summary>
    /// Whether this execution should name a prepared statement instead of carrying its SQL, registering
    /// it first if this is the execution that tips it over the threshold.
    ///
    /// <para>Registration is awaited rather than started in the background because the whole point is
    /// that <em>this</em> execution and the ones after it are cheap; firing it off and running inline
    /// anyway would leave a busy statement racing its own warm-up. It costs one extra round trip, once
    /// per statement per endpoint.</para>
    ///
    /// <para>The caller passes the endpoint it has already resolved rather than letting this resolve its
    /// own. A REST handle is node-local and <see cref="GetEndpoint"/> rotates through the pool, so
    /// registering against a freshly drawn endpoint would routinely prepare on one node and execute on
    /// another — correct, because the transport re-prepares on the node it lands on, but a wasted round
    /// trip every single time.</para>
    /// </summary>
    private async ValueTask<bool> ShouldPrepareAsync(string endpoint, CancellationToken cancellationToken)
    {
        if (CommandType != CommandType.Text || !IsPreparableStatement(CommandText))
            return false;

        CamusPreparedStatementPolicy policy = builder.PreparedStatements;

        if (policy.IsDisabled)
            return false;

        string database = builder.Config["Database"];

        PrepareDecision decision = policy.Decide(database, CommandText, out (string Database, string Sql)? evicted);

        Release(endpoint, evicted);

        return decision switch
        {
            PrepareDecision.Yes => true,
            PrepareDecision.Register => await RegisterAsync(policy, endpoint, database, CommandText, cancellationToken).ConfigureAwait(false),
            _ => false,
        };
    }

    /// <summary>
    /// Registers a statement and records the outcome, returning whether it may now be executed prepared.
    ///
    /// <para>A registration failure is never surfaced: preparing is an optimization, and the statement
    /// runs inline exactly as it would have if the driver had not tried. What the failure <em>does</em>
    /// decide is whether to try again — a refusal specific to this statement (not preparable, a full
    /// server-side cap) stops asking for that statement, while anything that suggests the node has no
    /// prepared-statement support at all stops asking for every statement, because one round trip per
    /// distinct SQL to relearn that is worse than not trying.</para>
    /// </summary>
    private async ValueTask<bool> RegisterAsync(
        CamusPreparedStatementPolicy policy, string endpoint, string database, string sql, CancellationToken cancellationToken)
    {
        try
        {
            await builder.GetTransport()
                .PrepareAsync(endpoint, database, sql, CommandTimeout, cancellationToken)
                .ConfigureAwait(false);

            policy.MarkPrepared(database, sql);
            return true;
        }
        catch (OperationCanceledException)
        {
            // The caller's cancellation, not a verdict on the statement — but the entry must not be left
            // mid-registration, or it would never be reconsidered.
            policy.Forget(database, sql);
            throw;
        }
        catch (Exception ex)
        {
            // Deliberately broad. Whatever went wrong, the statement is about to run inline and report
            // any real problem itself; the only decision left here is whether asking again is worth a
            // round trip, and for this statement it is not.
            policy.MarkRefused(database, sql);

            if (ex is CamusException camus && IsUnsupported(camus))
                policy.Disable();

            return false;
        }
    }

    /// <summary>
    /// True when the failure says the node does not implement prepared statements at all, rather than
    /// declining this particular statement — a route that does not exist over REST, or an RPC the server
    /// does not implement over gRPC. Both surface under the generic transport code, because a server old
    /// enough not to have the feature is also too old to have a specific code for refusing it; a server
    /// that does have it declines individual statements with a <c>CADB05xx</c> code instead.
    ///
    /// <para>A heuristic, and only ever used to answer "is asking again worth a round trip?". Reading it
    /// wrong costs at most one wasted registration attempt per statement, never a failed statement.</para>
    /// </summary>
    private static bool IsUnsupported(CamusException ex)
        => ex.Code == "CADB0000"
            && (ex.Message.Contains("404", StringComparison.Ordinal)
                || ex.Message.Contains("Unimplemented", StringComparison.OrdinalIgnoreCase));

    /// <summary>Releases a statement the policy evicted, so the server stops holding a handle this client
    /// has stopped using. Best-effort and off the caller's path: it is bookkeeping, not part of the
    /// statement being run. Takes the endpoint the caller already resolved rather than drawing a new one,
    /// which would rotate the endpoint pool as a side effect of housekeeping.</summary>
    private void Release(string endpoint, (string Database, string Sql)? evicted)
    {
        if (evicted is not { } statement)
            return;

        ICamusTransport transport = builder.GetTransport();

        _ = Task.Run(async () =>
        {
            try
            {
                await transport.ClosePreparedAsync(endpoint, statement.Database, statement.Sql, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch
            {
                // The handle expires on its own; failing to close it early is not worth reporting.
            }
        });
    }

    /// <summary>
    /// The statements the server accepts a registration for: the repeatable data statements, whose whole
    /// point is running many times with different values. Schema and administration statements are
    /// one-shot and are excluded on the server too, so this list mirrors that rather than guessing.
    /// </summary>
    private static readonly string[] PreparablePrefixes =
    [
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "SHOW",
    ];

    private static bool IsPreparableStatement(string sql) => StartsWithAny(sql, PreparablePrefixes);

    protected override DbParameter CreateDbParameter()
    {
        return new CamusParameter();
    }
}
