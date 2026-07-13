
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Translates <see cref="Regex"/> static methods and the <c>EF.Functions.Regexp*</c> helpers onto
/// CamusDB's native <c>regexp_*</c> scalar functions.
///
/// <para><see cref="Regex.IsMatch(string, string)"/> maps to <c>regexp_like</c> — the functional form of
/// the <c>~</c> operator, with identical semantics on the server. <see cref="Regex.Replace(string, string, string)"/>
/// maps to <c>regexp_replace</c> with the <c>g</c> (replace-all) flag, matching .NET's replace-every-match
/// behavior. A <see cref="RegexOptions"/> argument is only honored when it is a compile-time constant made up
/// of flags CamusDB can express; anything else returns <see langword="null"/> so EF reports the call as
/// untranslatable rather than silently changing the match semantics.</para>
/// </summary>
public sealed class CamusRegexMethodTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo IsMatch =
        typeof(Regex).GetMethod(nameof(Regex.IsMatch), [typeof(string), typeof(string)])!;
    private static readonly MethodInfo IsMatchOptions =
        typeof(Regex).GetMethod(nameof(Regex.IsMatch), [typeof(string), typeof(string), typeof(RegexOptions)])!;
    private static readonly MethodInfo StaticReplace =
        typeof(Regex).GetMethod(nameof(Regex.Replace), [typeof(string), typeof(string), typeof(string)])!;
    private static readonly MethodInfo StaticReplaceOptions =
        typeof(Regex).GetMethod(nameof(Regex.Replace), [typeof(string), typeof(string), typeof(string), typeof(RegexOptions)])!;

    // RegexOptions this provider can express as CamusDB flag characters. Options with no bearing on
    // match results (None, Compiled, CultureInvariant) are treated as a no-op; anything else is rejected.
    private const RegexOptions TranslatableOptions =
        RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline |
        RegexOptions.IgnorePatternWhitespace | RegexOptions.Compiled | RegexOptions.CultureInvariant;

    private readonly ISqlExpressionFactory _sql;

    public CamusRegexMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sql = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType == typeof(Regex))
            return TranslateBcl(method, arguments);

        if (method.DeclaringType == typeof(CamusDbFunctionsExtensions))
            return TranslateEfFunction(method, arguments);

        return null;
    }

    private SqlExpression? TranslateBcl(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        if (method == IsMatch)
            return Function("regexp_like", typeof(bool), arguments[0], arguments[1]);

        if (method == IsMatchOptions)
        {
            string? flags = FlagsFromOptions(arguments[2], global: false);
            return flags is null ? null : LikeWithFlags(arguments[0], arguments[1], flags);
        }

        if (method == StaticReplace)
            // .NET Regex.Replace replaces every match; CamusDB needs the explicit 'g' flag for that.
            return Function("regexp_replace", typeof(string),
                arguments[0], arguments[1], arguments[2], Const("g"));

        if (method == StaticReplaceOptions)
        {
            string? flags = FlagsFromOptions(arguments[3], global: true);
            return flags is null
                ? null
                : Function("regexp_replace", typeof(string),
                    arguments[0], arguments[1], arguments[2], Const(flags));
        }

        return null;
    }

    private SqlExpression? TranslateEfFunction(MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        // arguments[0] is the DbFunctions receiver; the real arguments follow.
        SqlExpression[] args = arguments.Skip(1).ToArray();

        return method.Name switch
        {
            nameof(CamusDbFunctionsExtensions.RegexpLike) => Function("regexp_like", typeof(bool), args),
            nameof(CamusDbFunctionsExtensions.RegexpReplace) => Function("regexp_replace", typeof(string), args),
            nameof(CamusDbFunctionsExtensions.RegexpCount) => Function("regexp_count", typeof(int), args),
            nameof(CamusDbFunctionsExtensions.RegexpSubstr) => Function("regexp_substr", typeof(string), args),
            nameof(CamusDbFunctionsExtensions.RegexpInstr) => Function("regexp_instr", typeof(int), args),
            _ => null,
        };
    }

    private SqlExpression LikeWithFlags(SqlExpression input, SqlExpression pattern, string flags)
        => flags.Length == 0
            ? Function("regexp_like", typeof(bool), input, pattern)
            : Function("regexp_like", typeof(bool), input, pattern, Const(flags));

    // A string literal carrying the default type mapping so the generator renders it as a quoted 'flags'.
    private SqlExpression Const(string value)
        => _sql.ApplyDefaultTypeMapping(_sql.Constant(value));

    // Converts a constant RegexOptions argument to a CamusDB flag string, or null when it is not a
    // constant or carries a flag the server can't express (so the caller falls back to no translation).
    private static string? FlagsFromOptions(SqlExpression optionsExpression, bool global)
    {
        if (optionsExpression is not SqlConstantExpression { Value: RegexOptions options })
            return null;

        if ((options & ~TranslatableOptions) != 0)
            return null;

        string flags = "";
        if (options.HasFlag(RegexOptions.IgnoreCase)) flags += "i";
        if (options.HasFlag(RegexOptions.Multiline)) flags += "m";
        if (options.HasFlag(RegexOptions.Singleline)) flags += "s";
        if (options.HasFlag(RegexOptions.IgnorePatternWhitespace)) flags += "x";
        if (global) flags += "g";
        return flags;
    }

    private SqlExpression Function(string name, Type returnType, params SqlExpression[] args)
        => _sql.Function(
            name,
            args,
            nullable: true,
            argumentsPropagateNullability: args.Select(_ => true).ToArray(),
            returnType: returnType);
}
