/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using CamusDB.Client.Transport;
using Flurl.Http;

namespace CamusDB.Client.Auth;

/// <summary>
/// <see cref="ICamusLoginClient"/> over the server's HTTP <c>/login</c> and <c>/logout</c> routes — the
/// only routes that are exempt from the server's own authentication middleware, and the only place a
/// password is ever put on the wire.
/// </summary>
internal sealed class RestLoginClient : ICamusLoginClient
{
    public async Task<CamusLoginResult> LoginAsync(string endpoint, string user, string password, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            CamusLoginRequest request = new()
            {
                User = user,
                Password = password
            };

            byte[] responseBytes = await endpoint
                .WithHeader("Accept", "application/json")
                .WithTimeout(timeoutSeconds)
                .AppendPathSegments("login")
                .PostAsync(CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusLoginRequest), cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusLoginResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusLoginResponse);

            if (response?.Status != "ok" || string.IsNullOrEmpty(response.Token))
                throw new CamusException(response?.Code ?? CamusAuthErrorCodes.AuthenticationFailed, response?.Message ?? "Authentication failed");

            return new CamusLoginResult(response.Token, ReadExpiry(response));
        }
        catch (FlurlHttpException ex)
        {
            throw await RestErrorTranslator.TranslateAsync(ex).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// How long the minted token is good for. Prefers the server-measured duration, which is immune to
    /// clock skew between client and server; falls back to the absolute instant, which is not; and returns
    /// null against a server that reports neither, leaving the driver on its configured fallback lifetime.
    /// A non-positive value is treated as absent rather than as an already-dead token.
    /// </summary>
    private static TimeSpan? ReadExpiry(CamusLoginResponse response)
    {
        if (response.ExpiresInSeconds is > 0 and long seconds)
            return TimeSpan.FromSeconds(seconds);

        if (response.ExpiresAtUnixMs is { } expiresAt)
        {
            TimeSpan remaining = DateTimeOffset.FromUnixTimeMilliseconds(expiresAt) - DateTimeOffset.UtcNow;

            if (remaining > TimeSpan.Zero)
                return remaining;
        }

        return null;
    }

    public async Task LogoutAsync(string endpoint, string token, int timeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            await endpoint
                .WithHeader("Accept", "application/json")
                .WithOAuthBearerToken(token)
                .WithTimeout(timeoutSeconds)
                .AppendPathSegments("logout")
                .PostAsync(content: null, cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }
        catch (FlurlHttpException ex)
        {
            throw await RestErrorTranslator.TranslateAsync(ex).ConfigureAwait(false);
        }
    }
}
