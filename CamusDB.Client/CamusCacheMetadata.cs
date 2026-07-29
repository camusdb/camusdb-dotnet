/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;

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

    /// <summary>
    /// Builds metadata straight from the query response's DOM — the single-pass parse the REST transport
    /// uses instead of the <see cref="CamusExecuteSqlQueryResponse"/> DTO. Reads the same fields as
    /// <see cref="FromResponse"/> and, like it, returns <see langword="null"/> when the response carried
    /// no cache fields (the query was not hinted).
    /// </summary>
    internal static CamusCacheMetadata? FromJson(JsonElement root)
    {
        string? status = ReadString(root, "cacheStatus");
        string? name = ReadString(root, "cacheName");

        if (status is null && name is null)
            return null;

        CamusHlcTimestamp? cachedAt = null;
        if (root.TryGetProperty("cachedAtHlc", out JsonElement hlc) && hlc.ValueKind == JsonValueKind.Object)
            cachedAt = new CamusHlcTimestamp
            {
                L = hlc.TryGetProperty("l", out JsonElement l) && l.TryGetInt64(out long lv) ? lv : 0,
                C = hlc.TryGetProperty("c", out JsonElement c) && c.TryGetUInt32(out uint cv) ? cv : 0,
            };

        long? ageMs = root.TryGetProperty("ageMs", out JsonElement age) && age.TryGetInt64(out long a) ? a : null;

        return new CamusCacheMetadata(status, ReadString(root, "cacheBypassReason"), name, cachedAt, ageMs);
    }

    private static string? ReadString(JsonElement root, string property)
        => root.TryGetProperty(property, out JsonElement value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;

    /// <summary>
    /// Builds metadata from the gRPC cache verdict carried by a query terminator. The server emits that
    /// message only for a <c>{cache=…}</c>-hinted statement, so an absent (<see langword="null"/>) message
    /// means the query was unhinted and this returns <see langword="null"/> — matching how the REST path
    /// reports no cache fields. The status/bypass strings are the same values REST uses, so both transports
    /// parse identically; the empty bypass string maps back to null.
    /// </summary>
    internal static CamusCacheMetadata? FromProto(Grpc.CacheMetadata? metadata)
    {
        if (metadata is null)
            return null;

        CamusHlcTimestamp? cachedAt = metadata.CachedAtHlc is { } hlc
            ? new CamusHlcTimestamp { L = hlc.L, C = hlc.C }
            : null;

        return new CamusCacheMetadata(
            string.IsNullOrEmpty(metadata.Status) ? null : metadata.Status,
            string.IsNullOrEmpty(metadata.BypassReason) ? null : metadata.BypassReason,
            string.IsNullOrEmpty(metadata.Name) ? null : metadata.Name,
            cachedAt,
            metadata.HasAgeMs ? metadata.AgeMs : null);
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
