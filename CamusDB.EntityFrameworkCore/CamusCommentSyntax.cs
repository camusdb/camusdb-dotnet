namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Validation and literal rendering for CamusDB object comments.
/// </summary>
/// <remarks>
/// CamusDB requires that SHOW CREATE TABLE emit DDL which re-parses to the identical comment, and
/// its string literals have no backslash-escape decoding — the lexer treats a backslash plus the
/// following character as one unit. Two shapes therefore have no representation and are rejected by
/// the server; we reject them here so the failure names the offending comment instead of surfacing
/// as an opaque InvalidInput midway through a migration.
/// </remarks>
internal static class CamusCommentSyntax
{
    /// <summary>Server-side bound; comments ride the replicated per-table metadata blob.</summary>
    private const int MaxLength = 65535;

    /// <summary>
    /// Renders <paramref name="comment"/> as a single-quoted SQL literal, doubling embedded quotes.
    /// </summary>
    public static string Literal(string comment, string target)
    {
        Validate(comment, target);
        return $"'{comment.Replace("'", "''")}'";
    }

    private static void Validate(string comment, string target)
    {
        if (comment.Length > MaxLength)
            throw new NotSupportedException(
                $"The comment on {target} is {comment.Length} characters; CamusDB allows at most {MaxLength}.");

        for (int i = 0; i < comment.Length; i++)
        {
            char c = comment[i];

            if (char.IsControl(c))
                throw new NotSupportedException(
                    $"The comment on {target} contains a control character (U+{(int)c:X4}) at position {i}; " +
                    "CamusDB comments cannot contain control characters such as newlines or tabs.");

            if (c != '\\')
                continue;

            // A backslash at the very end, or immediately before a quote, would escape the literal's
            // closing quote once the comment is re-emitted by SHOW CREATE TABLE.
            if (i == comment.Length - 1)
                throw new NotSupportedException(
                    $"The comment on {target} ends with a backslash; CamusDB cannot round-trip that through DDL.");

            if (comment[i + 1] == '\'')
                throw new NotSupportedException(
                    $"The comment on {target} contains a backslash immediately before a quote at position {i}; " +
                    "CamusDB cannot round-trip that through DDL.");
        }
    }
}
