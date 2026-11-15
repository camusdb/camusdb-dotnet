/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Adjusts the LINQ operator translation for CamusDB: <c>RightJoin</c>, and <c>Contains</c>, <c>Count</c>
/// and <c>Any</c> on an array column.
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
///
/// <para>
/// An array column is a primitive collection in EF Core. The server has no function that turns an array
/// into rows, so the provider translates only the operators that read the array as a whole:
/// <c>tags.Contains(x)</c> is <c>array_contains(tags, x)</c>, <c>tags.Count()</c> is <c>cardinality(tags)</c>,
/// and <c>tags.Any()</c> is <c>cardinality(tags) &gt; 0</c>. Every other query over the elements reaches
/// the SQL generator as the <see cref="ArrayElementsFunction"/> table, and the generator refuses it.
/// </para>
/// </summary>
public class CamusQueryableMethodTranslatingExpressionVisitor : RelationalQueryableMethodTranslatingExpressionVisitor
{
    /// <summary>The name of the table that stands for the elements of an array column. It is not a server
    /// function: <see cref="CamusQuerySqlGenerator"/> refuses a query that still reads it.</summary>
    internal const string ArrayElementsFunction = "camus_array_elements";

    private readonly RelationalQueryCompilationContext _queryCompilationContext;
    private readonly ISqlExpressionFactory _sqlExpressionFactory;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public CamusQueryableMethodTranslatingExpressionVisitor(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext)
    {
        _queryCompilationContext = queryCompilationContext;
        _sqlExpressionFactory = relationalDependencies.SqlExpressionFactory;
        _typeMappingSource = relationalDependencies.TypeMappingSource;
    }

    protected CamusQueryableMethodTranslatingExpressionVisitor(CamusQueryableMethodTranslatingExpressionVisitor parentVisitor)
        : base(parentVisitor)
    {
        _queryCompilationContext = parentVisitor._queryCompilationContext;
        _sqlExpressionFactory = parentVisitor._sqlExpressionFactory;
        _typeMappingSource = parentVisitor._typeMappingSource;
    }

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

    /// <summary>
    /// An array column as a table of its elements, so that EF Core can compose <c>Contains</c>,
    /// <c>Count</c> and <c>Any</c> over it. A parameter collection keeps the translation of the base,
    /// which binds each value as its own parameter.
    /// </summary>
#pragma warning disable EF1001 // SelectExpression's constructor is internal; providers use it to build a collection source.
    protected override ShapedQueryExpression? TranslatePrimitiveCollection(SqlExpression sqlExpression, IProperty? property, string tableAlias)
    {
        if (property is null
            || sqlExpression.TypeMapping is not CamusArrayTypeMapping { ElementTypeMapping: RelationalTypeMapping elementMapping }
            || sqlExpression.Type.GetElementType() is not { } elementType)
            return base.TranslatePrimitiveCollection(sqlExpression, property, tableAlias);

        TableValuedFunctionExpression elements = new(tableAlias, ArrayElementsFunction, [sqlExpression]);
        ColumnExpression value = new("value", tableAlias, elementType, elementMapping, nullable: true);
        SelectExpression select = new([elements], value, [], _queryCompilationContext.SqlAliasManager);

        Type nullableType = elementType.IsValueType && Nullable.GetUnderlyingType(elementType) is null
            ? typeof(Nullable<>).MakeGenericType(elementType)
            : elementType;
        Expression shaper = new ProjectionBindingExpression(select, new ProjectionMember(), nullableType);
        if (shaper.Type != elementType)
            shaper = Expression.Convert(shaper, elementType);

        return new ShapedQueryExpression(select, shaper);
    }

    protected override ShapedQueryExpression? TranslateContains(ShapedQueryExpression source, Expression item)
    {
        if (TryGetArray(source, out SqlExpression? array)
            && TranslateExpression(item, applyDefaultTypeMapping: false) is { } value)
        {
            value = _sqlExpressionFactory.ApplyTypeMapping(value, (RelationalTypeMapping?)array.TypeMapping!.ElementTypeMapping);
            return Scalar(source, Function("array_contains", typeof(bool), array, value), typeof(bool));
        }

        return base.TranslateContains(source, item);
    }

    protected override ShapedQueryExpression? TranslateCount(ShapedQueryExpression source, LambdaExpression? predicate)
        => predicate is null && TryGetArray(source, out SqlExpression? array)
            ? Scalar(source, Function("cardinality", typeof(int), array), typeof(int))
            : base.TranslateCount(source, predicate);

    protected override ShapedQueryExpression? TranslateLongCount(ShapedQueryExpression source, LambdaExpression? predicate)
        => predicate is null && TryGetArray(source, out SqlExpression? array)
            ? Scalar(source, Function("cardinality", typeof(long), array), typeof(long))
            : base.TranslateLongCount(source, predicate);

    protected override ShapedQueryExpression? TranslateAny(ShapedQueryExpression source, LambdaExpression? predicate)
    {
        if (predicate is null && TryGetArray(source, out SqlExpression? array))
        {
            SqlExpression nonEmpty = _sqlExpressionFactory.GreaterThan(
                Function("cardinality", typeof(int), array),
                _sqlExpressionFactory.Constant(0, _typeMappingSource.FindMapping(typeof(int))));
            return Scalar(source, nonEmpty, typeof(bool));
        }

        return base.TranslateAny(source, predicate);
    }

    // The array of an untouched element table: no filter, order, bound or grouping, and the element itself
    // as the projection (not tags.Select(t => t + 1)).
    private static bool TryGetArray(ShapedQueryExpression source, [System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out SqlExpression? array)
    {
        array = null;
        if (source.QueryExpression is not SelectExpression
            {
                Tables: [TableValuedFunctionExpression { Name: ArrayElementsFunction, Arguments: [var column] } elements],
                Predicate: null, GroupBy: [], Having: null, IsDistinct: false, Limit: null, Offset: null, Orderings: [],
            } select)
            return false;

        Expression shaper = source.ShaperExpression is UnaryExpression { NodeType: ExpressionType.Convert, Operand: var inner }
            ? inner
            : source.ShaperExpression;

        if (shaper is not ProjectionBindingExpression binding
            || select.GetProjection(binding) is not ColumnExpression { Name: "value" } value
            || value.TableAlias != elements.Alias)
            return false;

        array = column;
        return true;
    }

    private SqlExpression Function(string name, Type returnType, params SqlExpression[] arguments)
        => _sqlExpressionFactory.Function(name, arguments, nullable: true,
            argumentsPropagateNullability: arguments.Select(_ => true).ToArray(),
            returnType, _typeMappingSource.FindMapping(returnType));

    private ShapedQueryExpression Scalar(ShapedQueryExpression source, SqlExpression projection, Type type)
    {
        SelectExpression select = new(projection, _queryCompilationContext.SqlAliasManager);
        Type nullableType = typeof(Nullable<>).MakeGenericType(type);
        return source.Update(select, Expression.Convert(new ProjectionBindingExpression(select, new ProjectionMember(), nullableType), type));
    }
#pragma warning restore EF1001
}
