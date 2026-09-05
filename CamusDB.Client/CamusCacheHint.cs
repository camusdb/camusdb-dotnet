/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

namespace CamusDB.Client;

/// <summary>
/// Helpers for building the CamusDB query result cache SQL fragments — the inline
/// <c>{cache=name[, ttl=…][, strict]}</c> hint appended after a table reference, and the
/// <c>EVICT CACHE</c> statement. These only assemble text; opting a query into the cache is
/// done by the server when the hint is present in the executed SQL.
/// </summary>
public static class CamusCacheHint
{
    /// <summary>Upper bound on a cache family name, so a name cannot be used to build an unbounded statement.</summary>
    private const int MaxNameLength = 128;

    /// <summary>
    /// Builds a <c>{cache=…}</c> hint to append immediately after a table reference (after the
    /// optional alias), e.g. <c>SELECT * FROM orders {cache=recent_orders, ttl=30s}</c>.
    /// </summary>
    /// <param name="name">
    /// The cache family name. Case-insensitive; lowercased by the server. Letters, digits and
    /// <c>_ - . :</c> only — the name is emitted into the hint as bare text, where nothing can be
    /// escaped, so anything else is refused rather than composed into the statement.
    /// </param>
    /// <param name="ttl">Optional per-entry TTL override. Emitted in milliseconds (e.g. <c>ttl=30000</c>).</param>
    /// <param name="strict">When <see langword="true"/>, adds <c>strict</c> to validate each hit against live storage.</param>
    public static string Build(string name, TimeSpan? ttl = null, bool strict = false)
    {
        CamusSqlSyntax.ValidateBareName(name, MaxNameLength, nameof(name));

        System.Text.StringBuilder sb = new();
        sb.Append("{cache=").Append(name);

        if (ttl is { } t)
        {
            long ms = (long)t.TotalMilliseconds;
            if (ms <= 0 || ms > int.MaxValue)
                throw new ArgumentOutOfRangeException(nameof(ttl), "TTL must be a positive value within int millisecond range.");

            sb.Append(", ttl=").Append(ms.ToString(CultureInfo.InvariantCulture));
        }

        if (strict)
            sb.Append(", strict");

        sb.Append('}');
        return sb.ToString();
    }

    /// <summary>
    /// Builds an <c>EVICT CACHE 'name'</c> statement for the current database.
    ///
    /// <para>The name is validated, not escaped. It reaches the server as a SQL literal, and CamusDB's
    /// lexer decodes no backslash escapes — a backslash before a quote would end the literal early and
    /// leave the rest of the name to parse as SQL. Holding the name to the same character set
    /// <see cref="Build"/> requires removes that shape entirely, which matters because this is a public
    /// API an application may reach with request-derived text (a tenant name keying a cache family).</para>
    /// </summary>
    public static string Evict(string name)
    {
        CamusSqlSyntax.ValidateBareName(name, MaxNameLength, nameof(name));

        return string.Concat("EVICT CACHE '", name, "'");
    }

    /// <summary>Builds an <c>EVICT CACHE ALL</c> statement for the current database.</summary>
    public static string EvictAll() => "EVICT CACHE ALL";
}
