/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Auth;

/// <summary>
/// The credential-exchange half of the protocol, kept separate from <see cref="Transport.ICamusTransport"/>
/// because it is the one exchange reachable without a token: REST serves it at <c>/login</c>, gRPC on the
/// dedicated <c>CamusAuth</c> service, and each transport implements it against its own endpoint. Keeping
/// it a separate interface makes that unauthenticated surface explicit, and lets
/// <see cref="CamusTokenProvider"/> be tested without a server.
/// </summary>
internal interface ICamusLoginClient
{
    /// <summary>
    /// Exchanges a password for a short-lived bearer token (<c>POST /login</c>). Throws
    /// <see cref="CamusException"/> with <see cref="CamusAuthErrorCodes.AuthenticationFailed"/> when the
    /// credentials are rejected.
    /// </summary>
    Task<CamusLoginResult> LoginAsync(string endpoint, string user, string password, int timeoutSeconds, CancellationToken cancellationToken);

    /// <summary>Revokes <paramref name="token"/> server-side (<c>POST /logout</c>).</summary>
    Task LogoutAsync(string endpoint, string token, int timeoutSeconds, CancellationToken cancellationToken);
}
