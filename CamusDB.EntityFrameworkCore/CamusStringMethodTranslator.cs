
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

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Translates the common <see cref="string"/> instance methods onto CamusDB's native scalar
/// functions. CamusDB ships boolean predicate functions (<c>contains</c>, <c>starts_with</c>,
/// <c>ends_with</c>) that take the search term as a plain argument, so — unlike a <c>LIKE</c>
/// pattern — no wildcard escaping is required and a non-constant (parameter/column) argument
/// translates just as cleanly as a literal.
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

    private readonly ISqlExpressionFactory _sql;

    public CamusStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sql = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
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

        return null;
    }

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
