/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Auth;

/// <summary>
/// The server's authentication/authorization error codes, as carried by <see cref="CamusException.Code"/>
/// (HTTP body <c>code</c> field, or the <c>camus-error-code</c> gRPC trailer).
/// </summary>
internal static class CamusAuthErrorCodes
{
    /// <summary>CADB0516 — no/invalid/expired token, unknown user, or wrong password (HTTP 401). Every
    /// authentication failure returns this same code so replies cannot be used to enumerate accounts.</summary>
    public const string AuthenticationFailed = "CADB0516";

    /// <summary>CADB0517 — authenticated, but lacking the privilege the statement requires (HTTP 403).
    /// Never retried: re-authenticating cannot grant a privilege.</summary>
    public const string InsufficientPrivilege = "CADB0517";

    /// <summary>CADB0518 — login rate limit or KDF saturation (HTTP 429).</summary>
    public const string TooManyAuthAttempts = "CADB0518";

    /// <summary>CADB0519 — a credential-bearing request arrived over a plaintext connection while the
    /// server requires TLS (HTTP 400).</summary>
    public const string InsecureTransport = "CADB0519";
}
