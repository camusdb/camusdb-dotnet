
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

        Dictionary<string, ColumnValue> commandParameters = new(Parameters.Count);

        foreach (CamusParameter parameter in Parameters)
        {
            if (parameter.ColumnType == ColumnType.Id || parameter.ColumnType == ColumnType.String)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, StrValue = parameter.Value!.ToString() });
            else if (parameter.ColumnType == ColumnType.Integer64)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, LongValue = (long)parameter.Value! });
            else if (parameter.ColumnType == ColumnType.Bool)
                commandParameters.Add(parameter.ParameterName ?? "", new() { Type = parameter.ColumnType, BoolValue = (bool)parameter.Value! });

        }

        var response = await endpoint
                                .WithTimeout(CommandTimeout)
                                .AppendPathSegments("insert")
                                .PostJsonAsync(new { databaseName = database, tableName = source, values = commandParameters })
                                .ReceiveString();

        Console.WriteLine(response);

        return 1;
    }
}
