/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Auth;

/// <summary>
/// A minted bearer token and, when the server reported one, how long it is good for.
///
/// <para><see cref="ExpiresIn"/> is a duration rather than an instant on purpose: the server measures it
/// when it issues the reply, so a client whose clock disagrees with the server's still renews on time.
/// It is null against a server predating the field, in which case the driver falls back to its configured
/// <c>TokenLifetime</c>.</para>
/// </summary>
internal readonly struct CamusLoginResult(string token, TimeSpan? expiresIn)
{
    public string Token { get; } = token;

    public TimeSpan? ExpiresIn { get; } = expiresIn;
}
