/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// The one place that renders a value or a name into SQL text, for the few paths that compose a
/// statement rather than bind a parameter (the admin DDL the gRPC transport composes, the cache hints,
/// the EF Core migration and DDL builders).
///
/// <para><b>Why the rules are what they are.</b> CamusDB's lexer performs no backslash-escape decoding:
/// it consumes a backslash plus the character after it as one unit. So a literal cannot carry a
/// backslash immediately before a quote — the pair is read as one unit and the doubled quote that was
/// meant to escape the quote instead closes the literal, leaving the rest of the value to parse as SQL.
/// A literal cannot end with a backslash either, for the same reason. Those two shapes have no spelling
/// at all, so they are refused here rather than emitted as text that parses as something else.</para>
///
/// <para>Identifiers are delimited with backticks, and a backtick inside a name is refused rather than
/// escaped: the server trims the delimiters instead of decoding a doubled backtick, so doubling would
/// neutralize nothing.</para>
///
/// <para>Parameter values never come through here. They travel as typed wire objects and are bound by
/// the server — see <see cref="CamusParameter"/>.</para>
/// </summary>
internal static class CamusSqlSyntax
{
    /// <summary>
    /// Renders <paramref name="value"/> as a single-quoted SQL literal, doubling embedded quotes.
    /// <paramref name="target"/> names the thing being rendered, so a refusal says which value is at
    /// fault instead of surfacing later as an opaque parse error.
    /// </summary>
    public static string Literal(string value, string target)
    {
        ValidateLiteral(value, target);

        return string.Concat("'", value.Replace("'", "''", StringComparison.Ordinal), "'");
    }

    /// <summary>
    /// Refuses the two shapes the server's lexer cannot round-trip: a backslash immediately before a
    /// single quote, and a trailing backslash. Both would let the value's own text end the literal.
    /// </summary>
    public static void ValidateLiteral(string value, string target)
    {
        ArgumentNullException.ThrowIfNull(value);

        // Fast path: a value with no backslash at all — which is nearly every value — needs no scan.
        int index = value.IndexOf('\\');
        if (index < 0)
            return;

        while (index >= 0)
        {
            if (index == value.Length - 1)
                throw new ArgumentException(
                    $"The value for {target} ends with a backslash. CamusDB's lexer reads a backslash and the " +
                    "character after it as one unit, so a trailing backslash would consume the literal's closing quote.",
                    target);

            if (value[index + 1] == '\'')
                throw new ArgumentException(
                    $"The value for {target} contains a backslash immediately before a quote at position {index}. " +
                    "CamusDB's lexer reads that pair as one unit, so the quote cannot be escaped.",
                    target);

            // The pair is consumed as one unit, so the character after the backslash cannot itself start
            // a new pair. Skip it, or "\\'" would be read here as a backslash before a quote.
            index = value.IndexOf('\\', index + 2);
        }
    }

    /// <summary>
    /// Quotes an identifier with backticks. A backtick inside the name is refused: CamusDB trims the
    /// delimiters instead of decoding a doubled backtick, so there is no spelling that survives, and
    /// emitting one anyway would break the statement out of its quoting.
    /// </summary>
    public static string DelimitIdentifier(string identifier, string parameterName)
    {
        ValidateIdentifier(identifier, parameterName);

        return string.Concat("`", identifier, "`");
    }

    /// <summary>Rejects an empty identifier and one carrying a backtick. See <see cref="DelimitIdentifier"/>.</summary>
    public static void ValidateIdentifier(string identifier, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("An identifier cannot be empty.", parameterName);

        if (identifier.Contains('`', StringComparison.Ordinal))
            throw new ArgumentException(
                $"The identifier '{identifier}' contains a backtick, which CamusDB cannot quote.", parameterName);
    }

    /// <summary>
    /// Checks a name that is emitted into SQL as bare text rather than as a quoted literal or a delimited
    /// identifier — today the query result cache family name, which appears both inside a
    /// <c>{cache=…}</c> hint and inside an <c>EVICT CACHE</c> literal.
    ///
    /// <para>Neither position can be escaped: the hint has no quoting at all, and the literal is subject
    /// to the backslash rule above. So the name is held to a character set that is safe in both — letters,
    /// digits, and <c>_ - . :</c> — which is what a cache family name is in practice. That also keeps a
    /// name that reaches this API from request-derived text (a tenant name, say) from reaching the
    /// parser as anything but a name.</para>
    /// </summary>
    public static void ValidateBareName(string name, int maxLength, string parameterName)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name, parameterName);

        if (name.Length > maxLength)
            throw new ArgumentException(
                $"'{parameterName}' is {name.Length} characters; at most {maxLength} are allowed.", parameterName);

        foreach (char c in name)
        {
            if (char.IsLetterOrDigit(c) || c is '_' or '-' or '.' or ':')
                continue;

            throw new ArgumentException(
                $"'{parameterName}' contains the character '{c}', which is not allowed. Use letters, digits, " +
                "or one of _ - . : — the name is emitted into SQL as text and cannot be escaped.",
                parameterName);
        }
    }
}
