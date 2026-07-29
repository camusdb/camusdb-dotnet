/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// Reply of <c>POST /login</c> and <c>POST /logout</c>. On success <c>status</c> is <c>ok</c> and, for
/// login, <c>token</c> holds the opaque bearer token (<c>camus_&lt;id&gt;.&lt;secret&gt;</c>). On failure
/// the server returns the same shape for every authentication failure — unknown user, wrong password,
/// bad token — so the reply cannot be used to enumerate accounts.
/// </summary>
internal sealed class CamusLoginResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("token")]
    public string? Token { get; set; }

    /// <summary>
    /// Unix epoch milliseconds (UTC) after which the token is rejected. Null on logout, on failure, and
    /// from servers predating the field.
    /// </summary>
    [JsonPropertyName("expiresAtUnixMs")]
    public long? ExpiresAtUnixMs { get; set; }

    /// <summary>
    /// Whole seconds until expiry as measured by the <b>server</b> when it issued the reply. Preferred
    /// over <see cref="ExpiresAtUnixMs"/>: it is a duration, so a client whose clock disagrees with the
    /// server's still renews on time.
    /// </summary>
    [JsonPropertyName("expiresInSeconds")]
    public long? ExpiresInSeconds { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}
