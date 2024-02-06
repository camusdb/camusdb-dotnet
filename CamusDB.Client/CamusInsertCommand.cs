
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Flurl.Http;
using Newtonsoft.Json;

namespace CamusDB.Client;

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

            CamusInsertRequest request = new()
            {
                DatabaseName = database,
                TableName = source,
                Values = GetCommandParameters()
            };

            if (transaction is not null)
            {
                request.TxnIdPT = transaction.TxnIdPT;
                request.TxnIdCounter = transaction.TxnIdCounter;
            }

            string jsonRequest = JsonConvert.SerializeObject(request);            

            CamusExecuteSqlNonQueryResponse response = await endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(CommandTimeout)
                                                        .AppendPathSegments("insert")
                                                        .PostStringAsync(jsonRequest, cancellationToken)
                                                        .ReceiveJson<CamusExecuteSqlNonQueryResponse>();            

            return response.Rows;
        }
        catch (FlurlHttpException ex)
        {
            string response = await ex.GetResponseStringAsync();          

            if (!string.IsNullOrEmpty(response))
            {              
                try
                {
                    CamusErrorResponse? errorResponse = System.Text.Json.JsonSerializer.Deserialize<CamusErrorResponse>(response);

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
}
