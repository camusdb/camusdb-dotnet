
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusExecuteSqlQueryResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("rows")]
    public List<Dictionary<string, ColumnValue>>? Rows { get; set; }

    /// <summary>Query result cache resolution: <c>hit</c>, <c>miss</c>, <c>bypass</c>, etc. Null when the query carried no cache hint.</summary>
    [JsonPropertyName("cacheStatus")]
    public string? CacheStatus { get; set; }

    /// <summary>Why the cache was bypassed or the entry was not published. Null otherwise.</summary>
    [JsonPropertyName("cacheBypassReason")]
    public string? CacheBypassReason { get; set; }

    /// <summary>The cache family name from the query hint. Present whenever the cache path was entered.</summary>
    [JsonPropertyName("cacheName")]
    public string? CacheName { get; set; }

    /// <summary>HLC timestamp at which a served entry was computed. Non-null only on a hit.</summary>
    [JsonPropertyName("cachedAtHlc")]
    public CamusHlcTimestamp? CachedAtHlc { get; set; }

    /// <summary>Approximate age of a served entry in milliseconds. Non-null only on a hit.</summary>
    [JsonPropertyName("ageMs")]
    public long? AgeMs { get; set; }
}