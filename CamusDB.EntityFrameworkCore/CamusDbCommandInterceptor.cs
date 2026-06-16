using System.Data.Common;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace CamusDB.EntityFrameworkCore;

// CamusDB does not support EXISTS. This interceptor rewrites:
//   SELECT EXISTS (SELECT 1 FROM t WHERE ...)
// into:
//   SELECT COUNT(*) FROM t WHERE ...
// EF Core reads the integer result and treats non-zero as true for AnyAsync.
//
// TODO: Once CamusDB supports the EXISTS syntax natively, remove this interceptor
// and its registration in CamusDBServiceCollectionExtensions.AddEntityFrameworkCamusDB.
public sealed class CamusDbCommandInterceptor : DbCommandInterceptor
{
    private static readonly Regex SelectExistsPrefix =
        new(@"^\s*SELECT\s+EXISTS\s*\(", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex SelectOne =
        new(@"\bSELECT\s+1\b", RegexOptions.Compiled | RegexOptions.IgnoreCase);

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

    private static void RewriteCommand(DbCommand command)
    {
        var rewritten = TryRewriteSelectExists(command.CommandText);
        if (rewritten != null)
            command.CommandText = rewritten;
    }

    // Rewrites "SELECT EXISTS (SELECT 1 FROM t WHERE ...)" to "SELECT COUNT(*) FROM t WHERE ...".
    // Returns null if the SQL doesn't match the expected pattern.
    private static string? TryRewriteSelectExists(string sql)
    {
        var prefixMatch = SelectExistsPrefix.Match(sql);
        if (!prefixMatch.Success)
            return null;

        // Find the closing ')' that matches the '(' opened by EXISTS (
        int innerStart = prefixMatch.Length;
        int depth = 1;
        int i = innerStart;
        while (i < sql.Length && depth > 0)
        {
            char c = sql[i];
            if (c == '(') depth++;
            else if (c == ')') { depth--; if (depth == 0) break; }
            i++;
        }

        // Ensure nothing meaningful follows the closing ')'
        if (sql.AsSpan(i + 1).Trim().Length > 0)
            return null;

        var inner = sql.Substring(innerStart, i - innerStart).Trim();
        return SelectOne.Replace(inner, "SELECT COUNT(*)");
    }
}
