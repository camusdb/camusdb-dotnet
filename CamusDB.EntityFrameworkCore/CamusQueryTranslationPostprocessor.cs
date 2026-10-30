/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Keeps a vector that is not loaded with its entity out of the <c>SELECT</c> that materializes the
/// entity. See the <c>loadWithEntity</c> parameter of
/// <see cref="CamusPropertyBuilderExtensions.IsVector(Microsoft.EntityFrameworkCore.Metadata.Builders.PropertyBuilder, int, bool)"/>.
///
/// <para>EF Core 10 has no "not loaded" property state. EF Core 11 adds one, together with a materializer
/// that skips the property. Here the rewrite runs after the relational postprocessor applied the
/// projection: the column stays at its position in the <c>SELECT</c>, but as <c>NULL</c>. The position
/// does not change, so the shaper reads the same ordinals, and it reads the value with a null check
/// because the projection is no longer a column.</para>
///
/// <para>The rewrite is conservative. It changes a projection only when every reader of that position is
/// the entity map of a vector that is not loaded. A query that also names the vector
/// (<c>Select(c => new { c, c.Embedding })</c>) shares the position, so the vector stays. A query whose
/// shaper has a form that the rewrite does not know (a collection include, a split query, a nested
/// query, <c>DISTINCT</c>) is left as it is, and then the vector is loaded.</para>
/// </summary>
public class CamusQueryTranslationPostprocessor : RelationalQueryTranslationPostprocessor
{
    public CamusQueryTranslationPostprocessor(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies,
        RelationalQueryCompilationContext queryCompilationContext)
        : base(dependencies, relationalDependencies, queryCompilationContext) { }

    public override Expression Process(Expression query)
    {
        Expression result = base.Process(query);

        return result is ShapedQueryExpression { QueryExpression: SelectExpression select } shaped
               && HasUnloadedVectors(QueryCompilationContext.Model)
            ? NullUnloadedVectors(shaped, select)
            : result;
    }

    // Whether a model has a vector that is not loaded with its entity. It is fixed once a model is built,
    // so it is found once per model. The table holds the model weakly.
    private static readonly ConditionalWeakTable<IModel, StrongBox<bool>> ModelHasUnloadedVectors = new();

    private static bool HasUnloadedVectors(IModel model)
        => ModelHasUnloadedVectors.GetValue(model, static m => new StrongBox<bool>(
            m.GetEntityTypes().Any(e => e.GetDeclaredProperties().Any(IsUnloadedVector)))).Value;

    private static bool IsUnloadedVector(IProperty property)
        => property.GetVectorDimensions() is not null && !property.IsLoadedWithEntity();

    private static ShapedQueryExpression NullUnloadedVectors(ShapedQueryExpression shaped, SelectExpression select)
    {
        if (select.IsDistinct)
            return shaped;

        ProjectionReaders readers = new(select);
        readers.Visit(shaped.ShaperExpression);
        if (readers.Unknown)
            return shaped;

        readers.Unloaded.ExceptWith(readers.Loaded);
        if (readers.Unloaded.Count == 0)
            return shaped;

        List<ProjectionExpression> projections = [.. select.Projection];
        foreach (int index in readers.Unloaded)
        {
            if (projections[index] is { Expression: ColumnExpression column } projection)
                projections[index] = projection.Update(new SqlConstantExpression(null, column.Type, column.TypeMapping));
        }

        SelectExpression updated = select.Update(
            select.Tables, select.Predicate, select.GroupBy, select.Having, projections,
            select.Orderings, select.Offset, select.Limit);

        return shaped.UpdateQueryExpression(updated);
    }

    /// <summary>
    /// Sorts the projection positions that the shaper reads. A position read by the entity map of a vector
    /// that is not loaded goes into <see cref="Unloaded"/>. Every other position that the shaper reads goes
    /// into <see cref="Loaded"/>. Any form that the visitor does not know sets <see cref="Unknown"/>.
    /// </summary>
    private sealed class ProjectionReaders(SelectExpression select) : ExpressionVisitor
    {
        public HashSet<int> Unloaded { get; } = [];

        public HashSet<int> Loaded { get; } = [];

        public bool Unknown { get; private set; }

        public override Expression? Visit(Expression? node)
            => Unknown ? node : base.Visit(node);

        protected override Expression VisitExtension(Expression node)
        {
            switch (node)
            {
                case ProjectionBindingExpression binding:
                    Read(binding);
                    return node;

                // Each of these reads through a select of its own, or through positions that a
                // collection description holds and this visitor does not open.
                case ShapedQueryExpression:
                case RelationalCollectionShaperExpression:
                case RelationalSplitCollectionShaperExpression:
                    Unknown = true;
                    return node;
            }

            try
            {
                return base.VisitExtension(node);
            }
            catch (ArgumentException)
            {
                // An extension node that cannot visit its children.
                Unknown = true;
                return node;
            }
        }

        private void Read(ProjectionBindingExpression binding)
        {
            // After the projection is applied, both a member binding and an index binding resolve to a
            // constant: a position for a scalar, or a property-to-position map for an entity.
            if (select.GetProjection(binding) is not ConstantExpression { Value: { } value })
            {
                Unknown = true;
                return;
            }

            switch (value)
            {
                case int index:
                    Loaded.Add(index);
                    break;

                case IReadOnlyDictionary<IPropertyBase, int> entityMap:
                    foreach ((IPropertyBase property, int index) in entityMap)
                    {
                        if (property is IProperty p && IsUnloadedVector(p))
                            Unloaded.Add(index);
                        else
                            Loaded.Add(index);
                    }
                    break;

                default:
                    Unknown = true;
                    break;
            }
        }
    }
}

/// <summary>Creates <see cref="CamusQueryTranslationPostprocessor"/>.</summary>
public class CamusQueryTranslationPostprocessorFactory : IQueryTranslationPostprocessorFactory
{
    public CamusQueryTranslationPostprocessorFactory(
        QueryTranslationPostprocessorDependencies dependencies,
        RelationalQueryTranslationPostprocessorDependencies relationalDependencies)
    {
        Dependencies = dependencies;
        RelationalDependencies = relationalDependencies;
    }

    protected virtual QueryTranslationPostprocessorDependencies Dependencies { get; }

    protected virtual RelationalQueryTranslationPostprocessorDependencies RelationalDependencies { get; }

    public virtual QueryTranslationPostprocessor Create(QueryCompilationContext queryCompilationContext)
        => new CamusQueryTranslationPostprocessor(
            Dependencies,
            RelationalDependencies,
            (RelationalQueryCompilationContext)queryCompilationContext);
}
