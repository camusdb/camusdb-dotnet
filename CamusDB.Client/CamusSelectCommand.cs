
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusExecuteSqlQueryResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    /// <summary>
    /// Positional result rows: a JSON array of row arrays, where <c>rows[r][c]</c> is the compact-raw
    /// value for <see cref="Columns"/><c>[c]</c>. Decoded against the declared column types by
    /// <see cref="CamusResultSet.FromWire"/>. Held as a raw <see cref="JsonElement"/> so cells are
    /// decoded once, positionally, without a per-cell object.
    /// </summary>
    [JsonPropertyName("rows")]
    public JsonElement Rows { get; set; }

    /// <summary>
    /// Ordered output column schema (name + declared <see cref="ColumnType"/>) for this query. Always
    /// present on row-producing responses, even when <see cref="Rows"/> is empty — this is what lets the
    /// client report the full result schema (field count, names, types) before the first row.
    /// </summary>
    [JsonPropertyName("columns")]
    public List<CamusColumnSchema>? Columns { get; set; }

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