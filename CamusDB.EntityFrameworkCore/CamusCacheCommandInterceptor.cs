/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data.Common;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace CamusDB.EntityFrameworkCore;

// Turns a query tagged via IQueryable.WithCache(...) into a CamusDB cache-hinted SELECT.
//
// EF Core renders a query tag as a leading SQL comment, e.g.:
//   -- camusdb:cache {cache=recent_orders, ttl=30000, strict}
//
//   SELECT `o`.`Id`, `o`.`Total`
//   FROM `orders` AS `o`
//   WHERE ...
//
// CamusDB's SQL parser does not understand the `{cache=…}` hint as a leading comment; the hint must
// sit immediately after the table reference. This interceptor strips the marker comment and injects
// the hint after the first `FROM <table> [AS <alias>]`, producing:
//
//   SELECT `o`.`Id`, `o`.`Total`
//   FROM `orders` AS `o` {cache=recent_orders, ttl=30000, strict}
//   WHERE ...
public sealed class CamusCacheCommandInterceptor : DbCommandInterceptor
{
    // Matches the marker comment line and captures the hint payload.
    private static readonly Regex CacheTag = new(
        @"^[ \t]*--[ \t]*" + Regex.Escape(CamusQueryableExtensions.TagMarker) + @"(?<hint>\{.*\})[ \t]*\r?\n?",
        RegexOptions.Compiled | RegexOptions.Multiline);

    // Matches the first `FROM <table> [AS <alias>]` so the hint can be appended after the alias.
    private static readonly Regex FromClause = new(
        @"\bFROM\s+`[^`]+`(?:\s+AS\s+`[^`]+`)?",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    public override InterceptionResult<DbDataReader> ReaderExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result)
    {
        RewriteCommand(command);
        return base.ReaderExecuting(command, eventData, result);
    }

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<DbDataReader> result,
        CancellationToken cancellationToken = default)
    {
        RewriteCommand(command);
        return base.ReaderExecutingAsync(command, eventData, result, cancellationToken);
    }

    public override InterceptionResult<object> ScalarExecuting(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<object> result)
    {
        RewriteCommand(command);
        return base.ScalarExecuting(command, eventData, result);
    }

    public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(
        DbCommand command,
        CommandEventData eventData,
        InterceptionResult<object> result,
        CancellationToken cancellationToken = default)
    {
        RewriteCommand(command);
        return base.ScalarExecutingAsync(command, eventData, result, cancellationToken);
    }

    private static void RewriteCommand(DbCommand command)
    {
        string? rewritten = TryInjectCacheHint(command.CommandText);
        if (rewritten != null)
            command.CommandText = rewritten;
    }

    // Removes the marker comment and injects the hint after the first table reference.
    // Returns null when the SQL carries no cache tag (or no table reference to attach it to).
    internal static string? TryInjectCacheHint(string sql)
    {
        Match tag = CacheTag.Match(sql);
        if (!tag.Success)
            return null;

        string hint = tag.Groups["hint"].Value;
        string withoutTag = CacheTag.Replace(sql, string.Empty);

        Match from = FromClause.Match(withoutTag);
        if (!from.Success)
            return withoutTag; // Tag stripped even if there is nowhere to attach the hint.

        return string.Concat(
            withoutTag.AsSpan(0, from.Index + from.Length),
            " ",
            hint,
            withoutTag.AsSpan(from.Index + from.Length));
    }
}
