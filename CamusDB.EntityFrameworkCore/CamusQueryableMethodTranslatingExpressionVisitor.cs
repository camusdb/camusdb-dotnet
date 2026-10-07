/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Adjusts the LINQ operator translation for CamusDB. Today that is <c>RightJoin</c> only.
///
/// In <c>A RIGHT JOIN B</c> the left operand <c>A</c> is the null-extended side. The relational base
/// keeps a <c>Where</c> on <c>A</c> in the statement's <c>WHERE</c>, which runs after the server pads the
/// unmatched <c>B</c> rows, so the filter removes them and the join returns inner-join rows. The filter
/// must run before the join, so <c>A</c> goes into a derived table first.
///
/// The same pushdown covers a left operand that is itself a join (<c>A JOIN B RIGHT JOIN C</c>). CamusDB
/// runs a right join as a left join with the operands swapped, and the right operand of a join must be
/// one table or one derived table, so the server refuses that chain (<c>CADB0533</c>). As a derived
/// table, the left operand is one table again.
/// </summary>
public class CamusQueryableMethodTranslatingExpressionVisitor : RelationalQueryableMethodTranslatingExpressionVisitor
{
    public CamusQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext) { }

    protected CamusQueryableMethodTranslatingExpressionVisitor(CamusQueryableMethodTranslatingExpressionVisitor parentVisitor)
        : base(parentVisitor) { }

    protected override QueryableMethodTranslatingExpressionVisitor CreateSubqueryVisitor()
        => new CamusQueryableMethodTranslatingExpressionVisitor(this);

    protected override ShapedQueryExpression? TranslateRightJoin(
        ShapedQueryExpression outer,
        ShapedQueryExpression inner,
        LambdaExpression outerKeySelector,
        LambdaExpression innerKeySelector,
        LambdaExpression resultSelector)
    {
        // Limit, Offset, Distinct and GroupBy already make the base push the outer query down.
        if (outer.QueryExpression is SelectExpression select && (select.Predicate != null || select.Tables.Count > 1))
            select.PushdownIntoSubquery();

        return base.TranslateRightJoin(outer, inner, outerKeySelector, innerKeySelector, resultSelector);
    }
}
