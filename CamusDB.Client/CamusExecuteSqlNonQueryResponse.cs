
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusExecuteSqlNonQueryResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("rows")]
    public int Rows { get; set; }

    /// <summary>Advisory routing metadata — absent unless the request negotiated it
    /// (<c>routingAcceptVersion = 1</c>) and the statement produced advice.</summary>
    [JsonPropertyName("routing")]
    public CamusRoutingMetadataResponse? Routing { get; set; }
}