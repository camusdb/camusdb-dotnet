/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// Cache resolution metadata a CamusDB server attaches to the response of a <c>{cache=…}</c>-hinted
/// <c>SELECT</c>. Every field is null/absent when the query carried no hint, so it never needs to be
/// inspected for ordinary queries. Surfaced on <see cref="CamusDataReader.CacheMetadata"/> and on
/// <see cref="CamusCommand.LastCacheMetadata"/> after the last executed reader query.
/// </summary>
public sealed class CamusCacheMetadata
{
    /// <summary>Parsed form of <see cref="RawStatus"/>: <c>hit</c>, <c>miss</c>, <c>bypass</c>, etc.</summary>
    public CamusCacheStatus Status { get; }

    /// <summary>Raw <c>cacheStatus</c> string exactly as reported by the server.</summary>
    public string? RawStatus { get; }

    /// <summary>
    /// Why the cache was bypassed or the entry was not published — e.g. <c>in-flight-write</c>,
    /// <c>cache-disabled</c>, <c>oversized-result</c>, <c>dependency-limit</c>. Null otherwise.
    /// </summary>
    public string? BypassReason { get; }

    /// <summary>The cache family name from the query hint; present whenever the cache path was entered.</summary>
    public string? Name { get; }

    /// <summary>HLC timestamp at which a served entry was computed. Non-null only on a hit.</summary>
    public CamusHlcTimestamp? CachedAtHlc { get; }

    /// <summary>Approximate wall-clock age of a served entry in milliseconds. Non-null only on a hit.</summary>
    public long? AgeMs { get; }

    /// <summary><see langword="true"/> when stored rows were served from the cache.</summary>
    public bool IsHit => Status == CamusCacheStatus.Hit;

    internal CamusCacheMetadata(
        string? rawStatus,
        string? bypassReason,
        string? name,
        CamusHlcTimestamp? cachedAtHlc,
        long? ageMs)
    {
        RawStatus = rawStatus;
        Status = ParseStatus(rawStatus);
        BypassReason = bypassReason;
        Name = name;
        CachedAtHlc = cachedAtHlc;
        AgeMs = ageMs;
    }

    /// <summary>
    /// Builds metadata from a query response, or returns <see langword="null"/> when the response
    /// carried no cache fields (the query was not hinted).
    /// </summary>
    internal static CamusCacheMetadata? FromResponse(CamusExecuteSqlQueryResponse response)
    {
        if (response.CacheStatus is null && response.CacheName is null)
            return null;

        return new CamusCacheMetadata(
            response.CacheStatus,
            response.CacheBypassReason,
            response.CacheName,
            response.CachedAtHlc,
            response.AgeMs);
    }

    private static CamusCacheStatus ParseStatus(string? status) => status switch
    {
        null => CamusCacheStatus.None,
        "hit" => CamusCacheStatus.Hit,
        "miss" => CamusCacheStatus.Miss,
        "bypass" => CamusCacheStatus.Bypass,
        "stale-revalidated" => CamusCacheStatus.StaleRevalidated,
        "evicted-before-publish" => CamusCacheStatus.EvictedBeforePublish,
        _ => CamusCacheStatus.Unknown,
    };
}
