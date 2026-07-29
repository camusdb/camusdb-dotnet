/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using Flurl.Http;

namespace CamusDB.Client.Transport;

/// <summary>
/// Turns a failed HTTP call into a <see cref="CamusException"/>: prefer the server's
/// <c>{status, code, message}</c> error body — which is what carries the <c>CADBxxxx</c> code the ADO
/// layer keys its retry/refresh decisions off — falling back to the raw response text and finally the
/// exception message. Shared by every REST caller (the transport and the login client) so all of them
/// surface identical exceptions.
/// </summary>
internal static class RestErrorTranslator
{
    public static async Task<CamusException> TranslateAsync(FlurlHttpException ex)
    {
        string response = await ex.GetResponseStringAsync().ConfigureAwait(false);

        if (!string.IsNullOrEmpty(response))
        {
            try
            {
                CamusErrorResponse? errorResponse = JsonSerializer.Deserialize(response, CamusJsonSerializerContext.Default.CamusErrorResponse);

                if (errorResponse is not null)
                    return new CamusException(errorResponse.Code ?? "CADB0000", errorResponse.Message ?? "");
            }
            catch (JsonException)
            {
            }

            return new CamusException("CADB0000", response);
        }

        return new CamusException("CADB0000", ex.Message);
    }
}
