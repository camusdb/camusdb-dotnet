using CamusDB.Client;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Validation and literal rendering for CamusDB object comments.
/// </summary>
/// <remarks>
/// CamusDB requires that SHOW CREATE TABLE emit DDL which re-parses to the identical comment, and
/// its string literals have no backslash-escape decoding — the lexer treats a backslash plus the
/// following character as one unit. Two shapes therefore have no representation and are rejected by
/// the server; <see cref="CamusSqlSyntax"/> rejects them for every literal the provider renders, and
/// this type adds the two rules that are specific to a comment: the length bound and the control
/// characters that the metadata blob cannot carry.
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

        return CamusSqlSyntax.Literal(comment, target);
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
        }
    }
}
