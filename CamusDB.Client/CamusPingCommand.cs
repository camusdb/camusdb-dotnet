
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using Flurl.Http;

namespace CamusDB.Client;

public class CamusPingCommand : CamusCommand
{
    public CamusPingCommand(string source, CamusConnectionStringBuilder builder, CamusConnection? connection = null) : base(source, builder, connection)
    {

    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        string endpoint = "";

        try
        {
            endpoint = GetEndpoint();

            string responseJson = await endpoint
                                                        .WithTimeout(CommandTimeout)
                                                        .AppendPathSegments("ping")
                                                        .GetStringAsync(cancellationToken: cancellationToken);

            CamusExecuteSqlNonQueryResponse? response = JsonSerializer.Deserialize(responseJson, CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryResponse);

            return response?.Status == "ok" ? 1 : 0;
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
