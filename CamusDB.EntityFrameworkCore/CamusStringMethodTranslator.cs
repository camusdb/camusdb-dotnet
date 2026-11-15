
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Translates the common <see cref="string"/> instance methods onto CamusDB's native scalar
/// functions. CamusDB ships boolean predicate functions (<c>contains</c>, <c>starts_with</c>,
/// <c>ends_with</c>) that take the search term as a plain argument, so — unlike a <c>LIKE</c>
/// pattern — no wildcard escaping is required and a non-constant (parameter/column) argument
/// translates just as cleanly as a literal.
///
/// <para>It also translates <c>Substring</c> and <c>IndexOf</c> (the server counts from 1), the indexer
/// <c>s[i]</c>, the <c>char</c> overloads of <c>Trim</c>, <c>Replace</c>, <c>Contains</c>, <c>StartsWith</c>
/// and <c>EndsWith</c>, and <see cref="string.IsNullOrWhiteSpace"/>.</para>
/// </summary>
public sealed class CamusStringMethodTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo StartsWith =
        typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!;
    private static readonly MethodInfo StartsWithComparison =
        typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string), typeof(StringComparison)])!;
    private static readonly MethodInfo EndsWith =
        typeof(string).GetMethod(nameof(string.EndsWith), [typeof(string)])!;
    private static readonly MethodInfo EndsWithComparison =
        typeof(string).GetMethod(nameof(string.EndsWith), [typeof(string), typeof(StringComparison)])!;
    private static readonly MethodInfo Contains =
        typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!;
    private static readonly MethodInfo ContainsComparison =
        typeof(string).GetMethod(nameof(string.Contains), [typeof(string), typeof(StringComparison)])!;
    private static readonly MethodInfo ToUpper =
        typeof(string).GetMethod(nameof(string.ToUpper), Type.EmptyTypes)!;
    private static readonly MethodInfo ToLower =
        typeof(string).GetMethod(nameof(string.ToLower), Type.EmptyTypes)!;
    private static readonly MethodInfo Trim =
        typeof(string).GetMethod(nameof(string.Trim), Type.EmptyTypes)!;
    private static readonly MethodInfo TrimStart =
        typeof(string).GetMethod(nameof(string.TrimStart), Type.EmptyTypes)!;
    private static readonly MethodInfo TrimEnd =
        typeof(string).GetMethod(nameof(string.TrimEnd), Type.EmptyTypes)!;
    private static readonly MethodInfo Replace =
        typeof(string).GetMethod(nameof(string.Replace), [typeof(string), typeof(string)])!;
    private static readonly MethodInfo ReplaceChar =
        typeof(string).GetMethod(nameof(string.Replace), [typeof(char), typeof(char)])!;
    private static readonly MethodInfo StartsWithChar =
        typeof(string).GetMethod(nameof(string.StartsWith), [typeof(char)])!;
    private static readonly MethodInfo EndsWithChar =
        typeof(string).GetMethod(nameof(string.EndsWith), [typeof(char)])!;
    private static readonly MethodInfo ContainsChar =
        typeof(string).GetMethod(nameof(string.Contains), [typeof(char)])!;
    private static readonly MethodInfo SubstringFrom =
        typeof(string).GetMethod(nameof(string.Substring), [typeof(int)])!;
    private static readonly MethodInfo SubstringFromLength =
        typeof(string).GetMethod(nameof(string.Substring), [typeof(int), typeof(int)])!;
    private static readonly MethodInfo IndexOfString =
        typeof(string).GetMethod(nameof(string.IndexOf), [typeof(string)])!;
    private static readonly MethodInfo IndexOfChar =
        typeof(string).GetMethod(nameof(string.IndexOf), [typeof(char)])!;
    private static readonly MethodInfo CharAt =
        typeof(string).GetMethod("get_Chars", [typeof(int)])!;
    private static readonly MethodInfo IsNullOrWhiteSpace =
        typeof(string).GetMethod(nameof(string.IsNullOrWhiteSpace), [typeof(string)])!;

    // Trim(char), Trim(params char[]) and the TrimStart/TrimEnd forms, with the server function of each.
    private static readonly Dictionary<MethodInfo, string> TrimCharacters = new()
    {
        [typeof(string).GetMethod(nameof(string.Trim), [typeof(char)])!] = "trim",
        [typeof(string).GetMethod(nameof(string.Trim), [typeof(char[])])!] = "trim",
        [typeof(string).GetMethod(nameof(string.TrimStart), [typeof(char)])!] = "ltrim",
        [typeof(string).GetMethod(nameof(string.TrimStart), [typeof(char[])])!] = "ltrim",
        [typeof(string).GetMethod(nameof(string.TrimEnd), [typeof(char)])!] = "rtrim",
        [typeof(string).GetMethod(nameof(string.TrimEnd), [typeof(char[])])!] = "rtrim",
    };

    private readonly ISqlExpressionFactory _sql;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public CamusStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
    {
        _sql = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        // string.IsNullOrWhiteSpace(s): s IS NULL OR regexp_like(s, '^\s*$'). A literal cannot hold a tab or a
        // line break, so the white space is matched with \s, the same set of characters as char.IsWhiteSpace.
        if (method == IsNullOrWhiteSpace)
        {
            SqlExpression value = _sql.ApplyDefaultTypeMapping(arguments[0]);
            return _sql.OrElse(
                _sql.IsNull(value),
                _sql.Function("regexp_like", [value, _sql.Constant(@"^\s*$", _typeMappingSource.FindMapping(typeof(string)))],
                    nullable: true, argumentsPropagateNullability: [true, true], typeof(bool)));
        }

        if (instance is null)
            return null;

        if (method == StartsWith)
            return Predicate("starts_with", instance, arguments[0], ignoreCase: false);
        if (method == StartsWithComparison)
            return Predicate("starts_with", instance, arguments[0], IsIgnoreCase(arguments[1]));
        if (method == EndsWith)
            return Predicate("ends_with", instance, arguments[0], ignoreCase: false);
        if (method == EndsWithComparison)
            return Predicate("ends_with", instance, arguments[0], IsIgnoreCase(arguments[1]));
        if (method == Contains)
            return Predicate("contains", instance, arguments[0], ignoreCase: false);
        if (method == ContainsComparison)
            return Predicate("contains", instance, arguments[0], IsIgnoreCase(arguments[1]));

        if (method == ToUpper)
            return Scalar("upper", typeof(string), instance);
        if (method == ToLower)
            return Scalar("lower", typeof(string), instance);
        if (method == Trim)
            return Scalar("trim", typeof(string), instance);
        if (method == TrimStart)
            return Scalar("ltrim", typeof(string), instance);
        if (method == TrimEnd)
            return Scalar("rtrim", typeof(string), instance);
        if (method == Replace)
            return Scalar("replace", typeof(string), instance, arguments[0], arguments[1]);
        if (method == ReplaceChar)
            return Scalar("replace", typeof(string), instance, AsText(arguments[0]), AsText(arguments[1]));
        if (method == StartsWithChar)
            return Predicate("starts_with", instance, AsText(arguments[0]), ignoreCase: false);
        if (method == EndsWithChar)
            return Predicate("ends_with", instance, AsText(arguments[0]), ignoreCase: false);
        if (method == ContainsChar)
            return Predicate("contains", instance, AsText(arguments[0]), ignoreCase: false);

        // .NET counts from 0 and the server counts from 1. .NET throws for a start or a length past the
        // end of the string; the server returns the part that exists.
        if (method == SubstringFrom)
            return Scalar("substring", typeof(string), instance, OneBased(arguments[0]));
        if (method == SubstringFromLength)
            return Scalar("substring", typeof(string), instance, OneBased(arguments[0]), _sql.ApplyDefaultTypeMapping(arguments[1]));

        // strpos counts from 1 and gives 0 when the text is not found, so the .NET index is strpos - 1.
        if (method == IndexOfString || method == IndexOfChar)
        {
            SqlExpression search = method == IndexOfChar ? AsText(arguments[0]) : arguments[0];
            return _sql.Subtract(
                Scalar("strpos", typeof(int), instance, search),
                _sql.Constant(1, _typeMappingSource.FindMapping(typeof(int))));
        }

        // s[i] is the character at i: substring(s, i + 1, 1).
        if (method == CharAt)
        {
            return _sql.Function("substring",
                [instance, OneBased(arguments[0]), _sql.Constant(1, _typeMappingSource.FindMapping(typeof(int)))],
                nullable: true, argumentsPropagateNullability: [true, true, true],
                typeof(char), _typeMappingSource.FindMapping(typeof(char)));
        }

        if (TrimCharacters.TryGetValue(method, out string? trim) && TrimSet(arguments[0]) is { } characters)
            return Scalar(trim, typeof(string), instance, characters);

        return null;
    }

    // A char argument as the one-character string that the server functions take. A constant becomes a
    // string literal; any other char already has the char mapping, which stores a char as a string.
    private SqlExpression AsText(SqlExpression character)
        => character is SqlConstantExpression { Value: char c }
            ? _sql.Constant(c.ToString(), _typeMappingSource.FindMapping(typeof(string)))
            : _sql.ApplyDefaultTypeMapping(character);

    private SqlExpression OneBased(SqlExpression index)
        => index is SqlConstantExpression { Value: int constant }
            ? _sql.Constant(constant + 1, _typeMappingSource.FindMapping(typeof(int)))
            : _sql.Add(_sql.ApplyDefaultTypeMapping(index), _sql.Constant(1, _typeMappingSource.FindMapping(typeof(int))));

    // The characters to trim, as one string: the server's trim(s, characters) removes each character of it.
    // Only a constant array has a known set; Trim(char) can take any char expression.
    private SqlExpression? TrimSet(SqlExpression argument)
        => argument switch
        {
            SqlConstantExpression { Value: char[] { Length: > 0 } characters }
                => _sql.Constant(new string(characters), _typeMappingSource.FindMapping(typeof(string))),
            SqlConstantExpression { Value: char[] } => null,
            _ when argument.Type == typeof(char) => AsText(argument),
            _ => null,
        };

    // A non-constant StringComparison can't be inspected at translation time; treat only the
    // known case-insensitive constants as ignore-case, otherwise fall back to ordinal.
    private static bool IsIgnoreCase(SqlExpression comparison)
        => comparison is SqlConstantExpression { Value: StringComparison c } &&
           c is StringComparison.OrdinalIgnoreCase
             or StringComparison.CurrentCultureIgnoreCase
             or StringComparison.InvariantCultureIgnoreCase;

    private SqlExpression Predicate(string name, SqlExpression instance, SqlExpression search, bool ignoreCase)
    {
        if (ignoreCase)
        {
            instance = Scalar("lower", typeof(string), instance);
            search = Scalar("lower", typeof(string), search);
        }

        return _sql.Function(
            name,
            [instance, search],
            nullable: true,
            argumentsPropagateNullability: [true, true],
            returnType: typeof(bool));
    }

    private SqlExpression Scalar(string name, Type returnType, params SqlExpression[] args)
        => _sql.Function(
            name,
            args,
            nullable: true,
            argumentsPropagateNullability: args.Select(_ => true).ToArray(),
            returnType: returnType);
}
