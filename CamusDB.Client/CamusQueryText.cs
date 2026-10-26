/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.RegularExpressions;

namespace CamusDB.Client;

/// <summary>
/// Reads a SQL text for the driver's spans: the tables it names, for <c>db.collection.name</c> and
/// <c>db.query.summary</c>, and a copy with its literal values removed, for <c>db.query.text</c> under
/// <c>TraceQueryText=Sanitized</c>.
/// </summary>
/// <remarks>
/// This is a scan of tokens, not a parse. It knows string, number and bytes literals, backtick
/// identifiers, <c>@</c> parameters and comments, and it takes a table from the name that follows
/// <c>FROM</c>, <c>JOIN</c>, <c>INTO</c>, <c>UPDATE</c> or <c>TABLE</c>. A comma join names only its
/// first table, and a common table expression is named as a table. Literal values never reach a
/// summary, because a table name is never a literal.
/// </remarks>
internal static partial class CamusQueryText
{
    /// <summary>The longest summary the driver writes. Longer summaries end at the last whole table.</summary>
    internal const int MaxSummaryLength = 255;

    /// <summary>The text a sanitized SQL text has in place of each literal value.</summary>
    internal const string Placeholder = "?";

    /// <summary>Keywords after which the next name is a table.</summary>
    private static readonly HashSet<string> TableKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "FROM", "JOIN", "INTO", "UPDATE", "TABLE",
    };

    /// <summary>Keywords that can follow a table keyword without being a table: <c>DO UPDATE SET</c>,
    /// <c>CREATE TABLE IF NOT EXISTS</c>, <c>TRUNCATE TABLE</c>, <c>DELETE FROM ONLY</c>.</summary>
    private static readonly HashSet<string> NotTables = new(StringComparer.OrdinalIgnoreCase)
    {
        "SET", "SELECT", "IF", "NOT", "EXISTS", "ONLY", "LATERAL", "WHERE", "VALUES", "AS", "TABLE",
    };

    /// <summary>Functions whose arguments contain <c>FROM</c> without naming a table.</summary>
    private static readonly HashSet<string> FromFunctions = new(StringComparer.OrdinalIgnoreCase)
    {
        "EXTRACT", "TRIM", "SUBSTRING", "SUBSTR", "POSITION", "OVERLAY",
    };

    /// <summary>
    /// The tables <paramref name="sql"/> names, in the order it names them, each once.
    /// </summary>
    internal static List<string> Tables(string sql)
    {
        List<Token> tokens = Tokenize(sql);
        List<string> tables = [];
        Stack<bool> fromIsArgument = new();
        bool expectTable = false;

        for (int index = 0; index < tokens.Count; index++)
        {
            Token token = tokens[index];

            if (token.Kind == TokenKind.Comment)
                continue;

            if (token.Kind == TokenKind.Punctuation)
            {
                if (token.Text == "(")
                {
                    Token? previous = index > 0 ? tokens[index - 1] : null;
                    fromIsArgument.Push(previous is { Kind: TokenKind.Word } && FromFunctions.Contains(previous.Value.Text));
                }
                else if (token.Text == ")" && fromIsArgument.Count > 0)
                {
                    fromIsArgument.Pop();
                }

                expectTable = false;
                continue;
            }

            if (expectTable)
            {
                expectTable = false;
                if (token.Kind == TokenKind.Identifier || (token.Kind == TokenKind.Word && !NotTables.Contains(token.Text)))
                {
                    string table = QualifiedName(tokens, ref index);
                    if (!tables.Contains(table, StringComparer.Ordinal))
                        tables.Add(table);
                    continue;
                }

                // CREATE TABLE IF NOT EXISTS t: the table follows the condition.
                if (token.Kind == TokenKind.Word && token.Text.Equals("IF", StringComparison.OrdinalIgnoreCase))
                {
                    while (index + 1 < tokens.Count && tokens[index + 1].Kind == TokenKind.Word
                           && tokens[index + 1].Text is var next
                           && (next.Equals("NOT", StringComparison.OrdinalIgnoreCase) || next.Equals("EXISTS", StringComparison.OrdinalIgnoreCase)))
                    {
                        index++;
                    }

                    expectTable = true;
                    continue;
                }
            }

            if (token.Kind == TokenKind.Word && TableKeywords.Contains(token.Text))
            {
                bool insideArgument = fromIsArgument.Count > 0 && fromIsArgument.Peek();
                expectTable = !(insideArgument && token.Text.Equals("FROM", StringComparison.OrdinalIgnoreCase));
            }
            else if (index == 0 && token.Kind == TokenKind.Word && token.Text.Equals("TRUNCATE", StringComparison.OrdinalIgnoreCase))
            {
                // TRUNCATE t, as well as TRUNCATE TABLE t.
                expectTable = true;
            }
        }

        return tables;
    }

    /// <summary>
    /// The <c>db.query.summary</c> of a statement: its operation followed by its tables, such as
    /// <c>SELECT orders customers</c>. <see langword="null"/> when the statement names no table.
    /// </summary>
    internal static string? Summary(string? operationName, IReadOnlyList<string> tables)
    {
        if (tables.Count == 0)
            return null;

        StringBuilder summary = new(operationName ?? string.Empty);
        foreach (string table in tables)
        {
            int length = summary.Length == 0 ? table.Length : summary.Length + 1 + table.Length;
            if (length > MaxSummaryLength)
                break;

            if (summary.Length > 0)
                summary.Append(' ');
            summary.Append(table);
        }

        return summary.Length == 0 ? null : summary.ToString();
    }

    /// <summary>
    /// <paramref name="sql"/> with each string, number and bytes literal replaced by <c>?</c>, each comment
    /// removed, and each list of two or more <c>?</c> written as <c>?, ...</c>. Identifiers, keywords and
    /// <c>@</c> parameters are kept as they are.
    /// </summary>
    internal static string Sanitize(string sql)
    {
        StringBuilder sanitized = new(sql.Length);
        int position = 0;
        foreach (Token token in Tokenize(sql))
        {
            sanitized.Append(sql, position, token.Start - position);
            sanitized.Append(token.Kind switch
            {
                TokenKind.Literal => Placeholder,
                TokenKind.Comment => " ",
                _ => sql.AsSpan(token.Start, token.Length),
            });
            position = token.Start + token.Length;
        }

        sanitized.Append(sql, position, sql.Length - position);
        return PlaceholderList().Replace(sanitized.ToString(), "?, ...");
    }

    [GeneratedRegex(@"\?(?:\s*,\s*\?)+")]
    private static partial Regex PlaceholderList();

    /// <summary>A name and the <c>.name</c> parts after it, without backticks: <c>shop.orders</c>.</summary>
    private static string QualifiedName(List<Token> tokens, ref int index)
    {
        StringBuilder name = new(tokens[index].Text);
        while (index + 2 < tokens.Count
               && tokens[index + 1] is { Kind: TokenKind.Punctuation, Text: "." }
               && tokens[index + 2].Kind is TokenKind.Identifier or TokenKind.Word)
        {
            name.Append('.').Append(tokens[index + 2].Text);
            index += 2;
        }

        return name.ToString();
    }

    private static List<Token> Tokenize(string sql)
    {
        List<Token> tokens = [];
        int index = 0;

        while (index < sql.Length)
        {
            char current = sql[index];
            char next = index + 1 < sql.Length ? sql[index + 1] : '\0';
            int start = index;

            if (char.IsWhiteSpace(current))
            {
                index++;
            }
            else if (current == '-' && next == '-')
            {
                while (index < sql.Length && sql[index] != '\n')
                    index++;
                tokens.Add(new Token(TokenKind.Comment, start, index - start, string.Empty));
            }
            else if (current == '/' && next == '*')
            {
                int end = sql.IndexOf("*/", index + 2, StringComparison.Ordinal);
                index = end < 0 ? sql.Length : end + 2;
                tokens.Add(new Token(TokenKind.Comment, start, index - start, string.Empty));
            }
            else if (current is '\'' or '"')
            {
                index = SkipQuoted(sql, index, current, backslashEscapes: false);
                tokens.Add(new Token(TokenKind.Literal, start, index - start, string.Empty));
            }
            else if ((current is 'e' or 'E') && next is '\'' or '"')
            {
                index = SkipQuoted(sql, index + 1, next, backslashEscapes: true);
                tokens.Add(new Token(TokenKind.Literal, start, index - start, string.Empty));
            }
            else if ((current is 'x' or 'X') && next == '\'')
            {
                index = SkipQuoted(sql, index + 1, '\'', backslashEscapes: false);
                tokens.Add(new Token(TokenKind.Literal, start, index - start, string.Empty));
            }
            else if (current == '`')
            {
                index = SkipQuoted(sql, index, '`', backslashEscapes: false);
                string text = sql.Substring(start + 1, Math.Max(0, index - start - 2)).Replace("``", "`", StringComparison.Ordinal);
                tokens.Add(new Token(TokenKind.Identifier, start, index - start, text));
            }
            else if (char.IsAsciiDigit(current) || (current == '.' && char.IsAsciiDigit(next)))
            {
                index = SkipNumber(sql, index);
                tokens.Add(new Token(TokenKind.Literal, start, index - start, string.Empty));
            }
            else if (IsWordStart(current) || (current == '@' && IsWordStart(next)))
            {
                index++;
                while (index < sql.Length && IsWordPart(sql[index]))
                    index++;

                TokenKind kind = current == '@' ? TokenKind.Parameter : TokenKind.Word;
                tokens.Add(new Token(kind, start, index - start, sql[start..index]));
            }
            else
            {
                index++;
                tokens.Add(new Token(TokenKind.Punctuation, start, 1, sql[start..index]));
            }
        }

        return tokens;
    }

    /// <summary>The index after the quoted run that opens at <paramref name="index"/>. A doubled quote
    /// is part of the run. A backslash escapes the next character only in an <c>E'...'</c> string: in
    /// any other string it is an ordinary character, as the server reads it.</summary>
    private static int SkipQuoted(string sql, int index, char quote, bool backslashEscapes)
    {
        index++;
        while (index < sql.Length)
        {
            char current = sql[index];
            if (current == '\\' && backslashEscapes)
            {
                index += 2;
                continue;
            }

            if (current == quote)
            {
                if (index + 1 < sql.Length && sql[index + 1] == quote)
                {
                    index += 2;
                    continue;
                }

                return index + 1;
            }

            index++;
        }

        return sql.Length;
    }

    private static int SkipNumber(string sql, int index)
    {
        if (sql[index] == '0' && index + 1 < sql.Length && sql[index + 1] is 'x' or 'X')
        {
            index += 2;
            while (index < sql.Length && char.IsAsciiHexDigit(sql[index]))
                index++;
            return index;
        }

        while (index < sql.Length && (char.IsAsciiDigit(sql[index]) || sql[index] == '.'))
            index++;

        if (index < sql.Length && sql[index] is 'e' or 'E')
        {
            int exponent = index + 1;
            if (exponent < sql.Length && sql[exponent] is '+' or '-')
                exponent++;
            if (exponent < sql.Length && char.IsAsciiDigit(sql[exponent]))
            {
                index = exponent;
                while (index < sql.Length && char.IsAsciiDigit(sql[index]))
                    index++;
            }
        }

        return index;
    }

    private static bool IsWordStart(char value) => char.IsLetter(value) || value == '_';

    private static bool IsWordPart(char value) => char.IsLetterOrDigit(value) || value is '_' or '$';

    private enum TokenKind
    {
        Word = 0,
        Identifier = 1,
        Parameter = 2,
        Literal = 3,
        Comment = 4,
        Punctuation = 5,
    }

    private readonly record struct Token(TokenKind Kind, int Start, int Length, string Text);
}
