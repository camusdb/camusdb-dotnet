/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// Body of <c>POST /login</c>. This is the only request that ever carries a password; every other
/// request carries the bearer token it returns.
/// </summary>
internal sealed class CamusLoginRequest
{
    [JsonPropertyName("user")]
    public string? User { get; set; }

    [JsonPropertyName("password")]
    public string? Password { get; set; }
}
