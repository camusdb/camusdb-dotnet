
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

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Adds CamusDB-specific handling on top of the relational translator. Today that means the
/// <c>string.Compare(a, b) &lt;op&gt; 0</c> / <c>a.CompareTo(b) &lt;op&gt; 0</c> family, the idiom
/// keyset (cursor) pagination is written with.
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
