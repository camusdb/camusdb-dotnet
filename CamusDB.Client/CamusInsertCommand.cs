
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
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
        string endpoint = "";

        try
        {
            endpoint = builder.GetEndpoint();
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

            string jsonRequest = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusInsertRequest);

            string responseJson = await endpoint
                                                        .WithHeader("Accept", "application/json")
                                                        .WithTimeout(CommandTimeout)
                                                        .AppendPathSegments("insert")
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
}
