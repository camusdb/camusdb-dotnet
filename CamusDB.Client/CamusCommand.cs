
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
using System.Text.Json;
using CamusDB.Core.Util.ObjectIds;
using Flurl.Http;

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

    public override CommandType CommandType { get; set; }

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

    private static bool IsDdlStatement(string sql)
    {
        ReadOnlySpan<char> trimmed = sql.AsSpan().TrimStart();
        foreach (string prefix in DdlPrefixes)
        {
            if (trimmed.Length >= prefix.Length &&
                trimmed[..prefix.Length].Equals(prefix.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    private static bool IsDmlStatement(string sql)
    {
        ReadOnlySpan<char> trimmed = sql.AsSpan().TrimStart();
        foreach (string prefix in DmlPrefixes)
        {
            if (trimmed.Length >= prefix.Length &&
                trimmed[..prefix.Length].Equals(prefix.AsSpan(), StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    protected Dictionary<string, ColumnValue> GetCommandParameters()
    {
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
            // the big-endian halves on its side (see ColumnValue's JsonConstructor).
            case ColumnType.Uuid when value is Guid gu:
                return new() { Type = columnType, StrValue = gu.ToString() };

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

    /// <inheritdoc />
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
        ExecuteDbDataReaderAsync(behavior, default).GetAwaiter().GetResult();

    /// <inheritdoc />
    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
    {
        if (IsDmlStatement(CommandText))
            return await ExecuteDmlAsReaderAsync(cancellationToken).ConfigureAwait(false);

        string endpoint = "";
        string database = builder.Config["Database"];

        try
        {
            endpoint = GetEndpoint();

            CamusExecuteSqlQueryRequest request = new()
            {
                DatabaseName = database,
                Sql = GetRequestTarget(),
                Parameters = GetCommandParameters()
            };

            if (transaction is not null)
            {
                request.TxnIdPT = transaction.TxnIdPT;
                request.TxnIdCounter = transaction.TxnIdCounter;
            }

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusExecuteSqlQueryRequest);


            string responseJson = await endpoint
                                        .WithHeader("Accept", "application/json")
                                        .WithTimeout(CommandTimeout)
                                        .AppendPathSegments("execute-sql-query")
                                        .PostAsync(CamusJsonContent.Create(jsonRequest), cancellationToken: cancellationToken)
                                        .ReceiveString();

            CamusExecuteSqlQueryResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusExecuteSqlQueryResponse);

            if (response?.Rows == null)
                throw new CamusException("CADB0000", "Empty result returned");

            LastCacheMetadata = CamusCacheMetadata.FromResponse(response);

            return new CamusDataReader(response.Rows, LastCacheMetadata);
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {

                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
    }

    private async Task<CamusDataReader> ExecuteDmlAsReaderAsync(CancellationToken cancellationToken)
    {
        string endpoint = "";

        try
        {
            endpoint = GetEndpoint();
            string database = builder.Config["Database"];

            CamusExecuteSqlNonQueryRequest request = new()
            {
                DatabaseName = database,
                Sql = GetRequestTarget(),
                Parameters = GetCommandParameters()
            };

            if (transaction is not null)
            {
                request.TxnIdPT = transaction.TxnIdPT;
                request.TxnIdCounter = transaction.TxnIdCounter;
            }

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryRequest);

            string responseJson = await endpoint
                                        .WithHeader("Accept", "application/json")
                                        .WithTimeout(CommandTimeout)
                                        .AppendPathSegments("execute-sql-non-query")
                                        .PostAsync(CamusJsonContent.Create(jsonRequest), cancellationToken: cancellationToken)
                                        .ReceiveString();

            CamusExecuteSqlNonQueryResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryResponse);

            if (response is null)
                throw new CamusException("CADB0000", "Empty result returned");

            return new CamusDataReader(response.Rows);
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {
                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
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

        string endpoint = "";

        try
        {
            endpoint = GetEndpoint();
            string database = builder.Config["Database"];

            CamusExecuteSqlNonQueryRequest request = new()
            {
                DatabaseName = database,
                Sql = GetRequestTarget(),
                Parameters = GetCommandParameters()
            };

            if (transaction is not null)
            {
                request.TxnIdPT = transaction.TxnIdPT;
                request.TxnIdCounter = transaction.TxnIdCounter;
            }

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryRequest);

            string responseJson = await endpoint
                                        .WithHeader("Accept", "application/json")
                                        .WithTimeout(CommandTimeout)
                                        .AppendPathSegments("execute-sql-non-query")
                                        .PostAsync(CamusJsonContent.Create(jsonRequest), cancellationToken: cancellationToken)
                                        .ReceiveString();

            CamusExecuteSqlNonQueryResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryResponse);

            if (response is null)
                throw new CamusException("CADB0000", "Empty result returned");

            return response.Rows;
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {

                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
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
        string endpoint = "";

        try
        {
            endpoint = GetEndpoint();
            string database = builder.Config["Database"];            

            CamusExecuteDDLRequest request = new()
            {
                DatabaseName = database,
                Sql = GetRequestTarget()
            };

            if (transaction is not null)
            {
                request.TxnIdPT = transaction.TxnIdPT;
                request.TxnIdCounter = transaction.TxnIdCounter;
            }

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusExecuteDDLRequest);

            string responseJson = await endpoint
                                    .WithHeader("Accept", "application/json")
                                    .WithTimeout(CommandTimeout)
                                    .AppendPathSegments("execute-sql-ddl")
                                    .PostAsync(CamusJsonContent.Create(jsonRequest), cancellationToken: cancellationToken)
                                    .ReceiveString();

            CamusExecuteDDLResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusExecuteDDLResponse);

            return response?.Status == "ok";
        }
        catch (FlurlHttpException ex)
        {
            CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

            string response = await ex.GetResponseStringAsync();

            if (!string.IsNullOrEmpty(response))
            {
                try
                {
                    CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                    if (errorResponse is not null)
                        throw new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
                }
                catch (JsonException)
                {

                }

                throw new CamusException("CADB0000", response);
            }

            throw new CamusException("CADB0000", ex.Message);
        }
    }

    public override object? ExecuteScalar()
    {
        using DbDataReader reader = ExecuteDbDataReader(CommandBehavior.SingleRow);

        if (!reader.Read() || reader.FieldCount == 0)
            return null;

        return reader.GetValue(0);
    }

    public override void Prepare()
    {
        // CamusDB does not currently expose a server-side prepare API.
    }

    protected override DbParameter CreateDbParameter()
    {
        return new CamusParameter();
    }
}
