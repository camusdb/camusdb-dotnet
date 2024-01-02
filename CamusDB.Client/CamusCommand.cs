
using System.Data;
using System.Data.Common;
using Flurl.Http;

namespace CamusDB.Client;

/// <summary>
/// Represents a SQL query or command to execute against
/// a Camus database.
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
    protected readonly string source;

    protected readonly CamusConnectionStringBuilder builder;

    public CamusCommand(string source, CamusConnectionStringBuilder builder)
    {
        this.source = source;
        this.builder = builder;
    }

    /// <summary>
    /// The parameters of the SQL statement or command.
    /// </summary>
    public new CamusParameterCollection Parameters { get; } = new CamusParameterCollection();

    public override string CommandText { get; set; } = "";

    public override int CommandTimeout { get; set; } = 10;

    public override CommandType CommandType { get; set; }

    public override bool DesignTimeVisible { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    public override UpdateRowSource UpdatedRowSource { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    protected override DbConnection? DbConnection { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    protected override DbParameterCollection DbParameterCollection => throw new NotImplementedException();

    protected override DbTransaction? DbTransaction { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

    public override void Cancel()
    {
        throw new NotImplementedException();
    }

    public object Clone()
    {
        throw new NotImplementedException();
    }

    public override int ExecuteNonQuery()
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        string endpoint = builder.Config["Endpoint"];
        string database = builder.Config["Database"];

        Dictionary<string, ColumnValue> commandParameters = new(Parameters.Count);

        foreach (CamusParameter parameter in Parameters)
        {
            if (parameter.ColumnType == ColumnType.Id || parameter.ColumnType == ColumnType.String)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, StrValue = parameter.Value!.ToString() });
            else if (parameter.ColumnType == ColumnType.Integer64 && parameter.Value is int)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, LongValue = (int)parameter.Value! });
            else if (parameter.ColumnType == ColumnType.Integer64 && parameter.Value is long)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, LongValue = (long)parameter.Value! });
            else if (parameter.ColumnType == ColumnType.Bool)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, BoolValue = (bool)parameter.Value! });

        }

        var response = await endpoint
                                .WithTimeout(CommandTimeout)
                                .AppendPathSegments("execute-sql-non-query")
                                .PostJsonAsync(new { databaseName = database, sql = source, parameters = commandParameters })
                                .ReceiveString();

        Console.WriteLine(response);

        return 1;
    }

    public override object? ExecuteScalar()
    {
        throw new NotImplementedException();
    }

    public override void Prepare()
    {
        throw new NotImplementedException();
    }

    protected override DbParameter CreateDbParameter()
    {
        throw new NotImplementedException();
    }

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        throw new NotImplementedException();
    }

    /*public async Task ExecuteNonQueryAsync()
    {
        
    }*/
}

