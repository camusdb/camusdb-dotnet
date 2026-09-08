
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// Wire shape of the optional <c>routing</c> object on a REST SQL response — present only when
/// the request negotiated routing metadata (<c>routingAcceptVersion = 1</c>) and the statement
/// produced advice. Deserialized on the DTO-based non-query path; the query path reads the same
/// fields straight off its streamed DOM via <see cref="CamusRoutingAdvice.FromJson"/>.
/// </summary>
internal sealed class CamusRoutingMetadataResponse
{
    [JsonPropertyName("version")]
    public int Version { get; set; }

    [JsonPropertyName("disposition")]
    public string? Disposition { get; set; }

    [JsonPropertyName("preferredNodeId")]
    public string? PreferredNodeId { get; set; }

    [JsonPropertyName("reuseScope")]
    public string? ReuseScope { get; set; }

    [JsonPropertyName("dependencyToken")]
    public string? DependencyToken { get; set; }

    [JsonPropertyName("maxAgeMs")]
    public int MaxAgeMs { get; set; }

    [JsonPropertyName("provenance")]
    public string? Provenance { get; set; }

    [JsonPropertyName("reason")]
    public string? Reason { get; set; }
}
