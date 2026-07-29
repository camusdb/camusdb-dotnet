/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Auth;

/// <summary>
/// What the driver was given to authenticate with. Either a user/password pair — which the driver
/// exchanges for a short-lived bearer token at <c>/login</c> and re-exchanges when the token expires —
/// or a token the caller obtained elsewhere and hands over verbatim (which the driver cannot renew).
///
/// <para>A value with neither is <see cref="None"/>: the driver sends no <c>Authorization</c> header at
/// all, which is the correct behavior against a server with authentication disabled (the default).</para>
/// </summary>
internal readonly struct CamusCredentials
{
    public static readonly CamusCredentials None = default;

    /// <summary>User name to authenticate as, or null when a token was supplied directly.</summary>
    public string? User { get; }

    /// <summary>The user's password. Only ever sent to <c>/login</c>, never with an ordinary statement.</summary>
    public string? Password { get; }

    /// <summary>A pre-obtained bearer token, used as-is. Mutually exclusive with <see cref="User"/>.</summary>
    public string? AccessToken { get; }

    private CamusCredentials(string? user, string? password, string? accessToken)
    {
        User = user;
        Password = password;
        AccessToken = accessToken;
    }

    public static CamusCredentials FromPassword(string user, string password) => new(user, password, null);

    public static CamusCredentials FromToken(string accessToken) => new(null, null, accessToken);

    /// <summary>True when there is anything to authenticate with.</summary>
    public bool IsSet => !string.IsNullOrEmpty(User) || !string.IsNullOrEmpty(AccessToken);

    /// <summary>True when the driver can mint a fresh token on its own (it holds the password).</summary>
    public bool CanRenew => !string.IsNullOrEmpty(User) && Password is not null;
}
