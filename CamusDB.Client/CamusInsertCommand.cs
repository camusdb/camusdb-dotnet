
using Flurl.Http;

namespace CamusDB.Client;

public class CamusInsertCommand : CamusCommand
{
    public CamusInsertCommand(string source, CamusConnectionStringBuilder builder) : base(source, builder)
    {

    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        string endpoint = builder.Config["Endpoint"];
        string database = builder.Config["Database"];        

        Dictionary<string, ColumnValue> columnValues = new(Parameters.Count);

        foreach (CamusParameter parameter in Parameters)
            columnValues.Add(
                parameter.ParameterName ?? "",
                new() { Type = parameter.ColumnType, Value = parameter.Value!.ToString() }
            );

        var response = await endpoint
                                .WithTimeout(CommandTimeout)
                                .AppendPathSegments("insert")
                                .PostJsonAsync(new { databaseName = database, tableName = source, values = columnValues })
                                .ReceiveString();

        Console.WriteLine(response);

        return 1;
    }
}
