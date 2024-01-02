
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Flurl.Http;
using Newtonsoft.Json;

namespace CamusDB.Client;

internal sealed class ExecuteSqlNonQueryResponse
{
    [JsonProperty("status")]
    public string? Status { get; set; }

    [JsonProperty("rows")]
    public int Rows { get; set; }
}

public class CamusInsertCommand : CamusCommand
{
    public CamusInsertCommand(string source, CamusConnectionStringBuilder builder) : base(source, builder)
    {

    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        try
        {
            string endpoint = builder.Config["Endpoint"];
            string database = builder.Config["Database"];

            Dictionary<string, ColumnValue> commandParameters = GetCommandParameters();

            ExecuteSqlNonQueryResponse response = await endpoint
                                                    .WithTimeout(CommandTimeout)
                                                    .AppendPathSegments("insert")
                                                    .PostJsonAsync(new { databaseName = database, tableName = source, values = commandParameters })
                                                    .ReceiveJson<ExecuteSqlNonQueryResponse>();            

            return response.Rows;
        }
        catch (FlurlHttpException ex)
        {
            var response = await ex.GetResponseStringAsync();

            throw new CamusException(response);
        }
    }
}
