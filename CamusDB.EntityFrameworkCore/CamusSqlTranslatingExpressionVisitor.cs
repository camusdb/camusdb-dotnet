
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Adds CamusDB-specific handling on top of the relational translator: the
/// <c>string.Compare(a, b) &lt;op&gt; 0</c> / <c>a.CompareTo(b) &lt;op&gt; 0</c> family, the idiom
/// keyset (cursor) pagination is written with, and <c>string.Format</c>, which is what an interpolated
/// string (<c>$"{a} {b}"</c>) becomes in a LINQ expression.
///
/// Other providers translate these by emitting a three-valued <c>CASE WHEN … THEN -1 …</c> scalar and
/// comparing it to zero. CamusDB's SQL dialect has no <c>CASE</c> expression, so that route is closed;
/// instead the whole <c>compare-then-test-against-zero</c> shape is recognised at the expression level
/// and collapsed into the equivalent direct comparison (<c>Compare(a, b) &lt; 0</c> becomes
/// <c>a &lt; b</c>). The zero constant may sit on either side — <c>0 &gt; Compare(a, b)</c> flips the
/// operator.
///
/// Ordering follows the server, which compares String/Id columns with
/// <see cref="StringComparison.Ordinal"/>. The culture-sensitive overloads therefore translate to
/// ordinal ordering as well — the same approximation every relational provider makes when it defers
/// ordering to the database's collation. The ignore-case forms are honoured by wrapping both operands
/// in <c>lower()</c>.
///
/// <para><c>string.Format</c> becomes a <c>||</c> concatenation of its literal parts and its arguments.
/// Only a format whose result in SQL is the same as in C# translates: a constant format with plain
/// <c>{n}</c> placeholders (no alignment, no format string such as <c>{0:N2}</c>), and arguments that are
/// strings or integers. A <c>NULL</c> argument gives an empty string, as in C#. A number of another type,
/// a date, a <see cref="Guid"/> or a <see cref="bool"/> has a different text in SQL than in the current
/// culture of C#, so such a call is left to the relational base: it runs on the client in the final
/// projection, and fails to translate anywhere else.</para>
/// </summary>
public class CamusSqlTranslatingExpressionVisitor : RelationalSqlTranslatingExpressionVisitor
{
    private static readonly MethodInfo CompareStrings =
        typeof(string).GetMethod(nameof(string.Compare), [typeof(string), typeof(string)])!;
    private static readonly MethodInfo CompareStringsIgnoreCase =
        typeof(string).GetMethod(nameof(string.Compare), [typeof(string), typeof(string), typeof(bool)])!;
    private static readonly MethodInfo CompareStringsComparison =
        typeof(string).GetMethod(nameof(string.Compare), [typeof(string), typeof(string), typeof(StringComparison)])!;
    private static readonly MethodInfo CompareToString =
        typeof(string).GetMethod(nameof(string.CompareTo), [typeof(string)])!;

    public CamusSqlTranslatingExpressionVisitor(
        RelationalSqlTranslatingExpressionVisitorDependencies dependencies,
        QueryCompilationContext queryCompilationContext,
        QueryableMethodTranslatingExpressionVisitor queryableMethodTranslatingExpressionVisitor)
        : base(dependencies, queryCompilationContext, queryableMethodTranslatingExpressionVisitor)
    {
    }

    protected override Expression VisitBinary(BinaryExpression binaryExpression)
        => TryTranslateStringComparison(binaryExpression, out SqlExpression? translated)
            ? translated
            : base.VisitBinary(binaryExpression);

    protected override Expression VisitMethodCall(MethodCallExpression methodCallExpression)
        => TryTranslateFormat(methodCallExpression, out SqlExpression? translated)
            ? translated
            : base.VisitMethodCall(methodCallExpression);

    /// <summary>
    /// Translates <c>string.Format(format, args)</c> to the concatenation of the literal parts of
    /// <c>format</c> and the arguments. Each part is an operand of a string <c>Add</c>, which the SQL
    /// generator writes as <c>||</c>; EF's nullability pass then wraps each nullable operand in
    /// <c>COALESCE(x, '')</c>, which is what C# gives for a <c>null</c> argument.
    /// </summary>
    private bool TryTranslateFormat(MethodCallExpression call, out SqlExpression translated)
    {
        translated = null!;

        if (!TryGetFormatCall(call, out string format, out IReadOnlyList<Expression> arguments))
            return false;

        List<(string? Literal, int Index)> parts = [];
        if (!TryParseFormat(format, arguments.Count, parts))
            return false;

        RelationalTypeMapping? stringMapping = Dependencies.TypeMappingSource.FindMapping(typeof(string));
        SqlExpression?[] sqlArguments = new SqlExpression?[arguments.Count];
        SqlExpression? result = null;

        foreach ((string? literal, int index) in parts)
        {
            SqlExpression part;

            if (literal is not null)
            {
                part = Dependencies.SqlExpressionFactory.Constant(literal, stringMapping);
            }
            else
            {
                if (sqlArguments[index] is null)
                {
                    Expression argument = arguments[index];
                    Type type = Nullable.GetUnderlyingType(argument.Type) ?? argument.Type;

                    if (Visit(argument) is not SqlExpression sqlArgument)
                        return false;

                    sqlArguments[index] = type == typeof(string)
                        ? sqlArgument
                        : Dependencies.SqlExpressionFactory.Convert(sqlArgument, typeof(string), stringMapping);
                }

                part = sqlArguments[index]!;
            }

            result = result is null ? part : Dependencies.SqlExpressionFactory.Add(result, part);
        }

        // A format with no part is "". A format that is one placeholder only has no Add for the
        // nullability pass to protect, so its NULL is turned into "" here.
        translated = result switch
        {
            null => Dependencies.SqlExpressionFactory.Constant("", stringMapping),
            SqlConstantExpression => result,
            SqlBinaryExpression => result,
            _ => Dependencies.SqlExpressionFactory.Coalesce(result, Dependencies.SqlExpressionFactory.Constant("", stringMapping)),
        };

        return true;
    }

    /// <summary>
    /// Recognises <c>string.Format(string, object…)</c> with a constant format, and yields the argument
    /// expressions without their conversion to <see cref="object"/>. The <see cref="IFormatProvider"/>
    /// overloads and an argument array that is not written in place are not recognised.
    /// </summary>
    private static bool TryGetFormatCall(MethodCallExpression call, out string format, out IReadOnlyList<Expression> arguments)
    {
        format = null!;
        arguments = null!;

        MethodInfo method = call.Method;

        if (method.DeclaringType != typeof(string) || method.Name != nameof(string.Format) || call.Arguments.Count < 2)
            return false;

        if (method.GetParameters()[0].ParameterType != typeof(string))
            return false;

        if (call.Arguments[0] is not ConstantExpression { Value: string constantFormat })
            return false;

        IEnumerable<Expression> raw;

        if (call.Arguments.Count == 2 && call.Arguments[1].Type == typeof(object[]))
        {
            if (call.Arguments[1] is not NewArrayExpression { NodeType: ExpressionType.NewArrayInit } array)
                return false;

            raw = array.Expressions;
        }
        else
        {
            raw = call.Arguments.Skip(1);
        }

        List<Expression> unwrapped = [];
        foreach (Expression argument in raw)
        {
            Expression operand = argument is UnaryExpression { NodeType: ExpressionType.Convert } convert && convert.Type == typeof(object)
                ? convert.Operand
                : argument;

            if (!IsFormattableInSql(operand.Type))
                return false;

            unwrapped.Add(operand);
        }

        format = constantFormat;
        arguments = unwrapped;
        return true;
    }

    // The argument types whose SQL text is the C# text: a string as it is, and an integer in its plain
    // decimal form. C# writes a negative integer with the NegativeSign of the current culture, which is
    // "-" in the invariant culture and in almost every other.
    private static bool IsFormattableInSql(Type type)
    {
        type = Nullable.GetUnderlyingType(type) ?? type;

        return type == typeof(string)
            || type == typeof(sbyte) || type == typeof(byte)
            || type == typeof(short) || type == typeof(ushort)
            || type == typeof(int) || type == typeof(uint)
            || type == typeof(long) || type == typeof(ulong);
    }

    /// <summary>
    /// Splits a composite format into literal parts and placeholder indexes. <c>{{</c> and <c>}}</c> are
    /// literal braces. A placeholder must be digits only and name an argument that exists; anything else
    /// (alignment, a format string, white space, a lone brace) refuses the whole format.
    /// </summary>
    internal static bool TryParseFormat(string format, int argumentCount, List<(string? Literal, int Index)> parts)
    {
        System.Text.StringBuilder literal = new();

        for (int i = 0; i < format.Length; i++)
        {
            char c = format[i];

            if (c == '{')
            {
                if (i + 1 < format.Length && format[i + 1] == '{')
                {
                    literal.Append('{');
                    i++;
                    continue;
                }

                int close = format.IndexOf('}', i + 1);
                if (close < 0)
                    return false;

                ReadOnlySpan<char> digits = format.AsSpan(i + 1, close - i - 1);
                if (digits.IsEmpty || digits.Length > 9)
                    return false;

                foreach (char d in digits)
                {
                    if (!char.IsAsciiDigit(d))
                        return false;
                }

                int index = int.Parse(digits, System.Globalization.CultureInfo.InvariantCulture);
                if (index >= argumentCount)
                    return false;

                if (literal.Length > 0)
                {
                    parts.Add((literal.ToString(), -1));
                    literal.Clear();
                }

                parts.Add((null, index));
                i = close;
            }
            else if (c == '}')
            {
                if (i + 1 < format.Length && format[i + 1] == '}')
                {
                    literal.Append('}');
                    i++;
                    continue;
                }

                return false;
            }
            else
            {
                literal.Append(c);
            }
        }

        if (literal.Length > 0)
            parts.Add((literal.ToString(), -1));

        return true;
    }

    private bool TryTranslateStringComparison(BinaryExpression binaryExpression, out SqlExpression translated)
    {
        translated = null!;

        ExpressionType op = binaryExpression.NodeType;

        if (op is not (ExpressionType.Equal or ExpressionType.NotEqual
            or ExpressionType.LessThan or ExpressionType.LessThanOrEqual
            or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual))
        {
            return false;
        }

        MethodCallExpression call;

        if (IsZero(binaryExpression.Right) && binaryExpression.Left is MethodCallExpression leftCall)
            call = leftCall;
        else if (IsZero(binaryExpression.Left) && binaryExpression.Right is MethodCallExpression rightCall)
        {
            call = rightCall;
            op = Flip(op);
        }
        else
            return false;

        if (!TryGetComparands(call, out Expression? left, out Expression? right, out bool ignoreCase))
            return false;

        if (Visit(left) is not SqlExpression sqlLeft || Visit(right) is not SqlExpression sqlRight)
            return false;

        if (ignoreCase)
        {
            sqlLeft = Lower(sqlLeft);
            sqlRight = Lower(sqlRight);
        }

        SqlExpression? binary = Dependencies.SqlExpressionFactory.MakeBinary(op, sqlLeft, sqlRight, typeMapping: null);

        if (binary is null)
            return false;

        translated = binary;
        return true;
    }

    /// <summary>
    /// Recognises the four <c>Compare</c>/<c>CompareTo</c> shapes and yields their two operands. A
    /// non-constant <see cref="StringComparison"/> (or <c>ignoreCase</c> flag) can't be inspected at
    /// translation time, so those are left untranslated rather than guessed at.
    /// </summary>
    private static bool TryGetComparands(
        MethodCallExpression call, out Expression left, out Expression right, out bool ignoreCase)
    {
        left = null!;
        right = null!;
        ignoreCase = false;

        MethodInfo method = call.Method;

        if (method == CompareToString && call.Object is not null)
        {
            left = call.Object;
            right = call.Arguments[0];
            return true;
        }

        if (method == CompareStrings)
        {
            left = call.Arguments[0];
            right = call.Arguments[1];
            return true;
        }

        if (method == CompareStringsIgnoreCase)
        {
            if (call.Arguments[2] is not ConstantExpression { Value: bool flag })
                return false;

            left = call.Arguments[0];
            right = call.Arguments[1];
            ignoreCase = flag;
            return true;
        }

        if (method == CompareStringsComparison)
        {
            if (call.Arguments[2] is not ConstantExpression { Value: StringComparison comparison })
                return false;

            left = call.Arguments[0];
            right = call.Arguments[1];
            ignoreCase = comparison is StringComparison.OrdinalIgnoreCase
                or StringComparison.CurrentCultureIgnoreCase
                or StringComparison.InvariantCultureIgnoreCase;
            return true;
        }

        return false;
    }

    private SqlExpression Lower(SqlExpression value)
        => Dependencies.SqlExpressionFactory.Function(
            "lower",
            [value],
            nullable: true,
            argumentsPropagateNullability: [true],
            returnType: typeof(string),
            typeMapping: value.TypeMapping);

    private static bool IsZero(Expression expression)
        => expression is ConstantExpression { Value: int and 0 };

    private static ExpressionType Flip(ExpressionType op)
        => op switch
        {
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            _ => op,   // Equal / NotEqual are symmetric
        };
}
