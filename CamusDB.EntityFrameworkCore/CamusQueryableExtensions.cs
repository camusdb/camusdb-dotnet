/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// CamusDB-specific LINQ query extensions.
/// </summary>
public static class CamusQueryableExtensions
{
    /// <summary>
    /// The marker that prefixes a query tag carrying a CamusDB cache hint. The command interceptor
    /// (<see cref="CamusCacheCommandInterceptor"/>) recognizes it, strips the comment, and injects
    /// the <c>{cache=…}</c> hint into the generated SQL.
    /// </summary>
    internal const string TagMarker = "camusdb:cache ";

    /// <summary>
    /// Opts this query into the CamusDB query result cache under the given family <paramref name="name"/>.
    /// The provider injects a <c>{cache=…}</c> hint into the generated SQL, so a later identical query
    /// (same shape, same bound values, same schema) can be served from the server's in-memory cache.
    /// </summary>
    /// <remarks>
    /// The cache only serves single-table, autocommit reads: a query with a join, or one run inside an
    /// explicit transaction, reads live storage and the hint is inert. Cache resolution is reported on
    /// each query's response — see <see cref="CamusCacheMetadata"/>.
    /// </remarks>
    /// <param name="source">The query to cache.</param>
    /// <param name="name">The cache family name. Case-insensitive.</param>
    /// <param name="ttl">Optional per-entry TTL override; defaults to the server's configured TTL.</param>
    /// <param name="strict">When <see langword="true"/>, the server validates each hit against live storage.</param>
    public static IQueryable<TEntity> WithCache<TEntity>(
        this IQueryable<TEntity> source,
        string name,
        TimeSpan? ttl = null,
        bool strict = false)
    {
        ArgumentNullException.ThrowIfNull(source);

        string hint = CamusCacheHint.Build(name, ttl, strict);
        return source.TagWith(TagMarker + hint);
    }
}
