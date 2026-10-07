using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

public class CamusQuerySqlGenerator : QuerySqlGenerator
{
    // The alias of the ExecuteUpdate/ExecuteDelete target while its columns are written bare. CamusDB's
    // UPDATE and DELETE take no alias on the target table, and they do not resolve a column qualified
    // by the table name either, so a target column is only ever addressable by its bare name.
    private string? _bareColumnAlias;

    public CamusQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies) { }

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Limit != null)
        {
            Sql.AppendLine().Append("LIMIT ");
            Visit(selectExpression.Limit);
        }

        if (selectExpression.Offset != null)
        {
            if (selectExpression.Limit == null)
                Sql.AppendLine().Append("LIMIT -1");

            Sql.Append(" OFFSET ");
            Visit(selectExpression.Offset);
        }
    }

    protected override Expression VisitColumn(ColumnExpression columnExpression)
    {
        if (_bareColumnAlias != null && columnExpression.TableAlias == _bareColumnAlias)
        {
            Sql.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(columnExpression.Name));
            return columnExpression;
        }

        return base.VisitColumn(columnExpression);
    }

    // CamusDB has no lateral join. EF writes one for a SelectMany or a join whose inner sequence refers to
    // the outer row in a way that is not an equality key, and the server answers only with a syntax error.
    protected override Expression VisitCrossApply(CrossApplyExpression crossApplyExpression)
        => throw ApplyNotSupported();

    protected override Expression VisitOuterApply(OuterApplyExpression outerApplyExpression)
        => throw ApplyNotSupported();

    // CamusDB has no window functions. EF writes ROW_NUMBER() for Take/Skip on a sequence that is joined
    // per outer row: a filtered Include, a SelectMany, a per-group First.
    protected override Expression VisitRowNumber(RowNumberExpression rowNumberExpression)
        => throw new InvalidOperationException(
            "CamusDB cannot translate this query: it needs ROW_NUMBER() OVER (...), and CamusDB has no window functions. " +
            "EF Core writes it for Take/Skip/First on a per-row sequence (a filtered Include, a SelectMany, a per-group First). " +
            "Load that sequence in a separate query and apply Take/Skip on the client.");

    private static InvalidOperationException ApplyNotSupported()
        => new(
            "CamusDB cannot translate this query: it needs a lateral join (CROSS APPLY / OUTER APPLY), which CamusDB does not support. " +
            "EF Core writes one when the inner sequence of a SelectMany or a join refers to the outer row outside an equality key. " +
            "Make the correlation an equality join key, or issue separate queries.");

    /// <summary>
    /// ExecuteUpdate. The server's form is <c>UPDATE t SET c = v[, ...] WHERE cond</c>: no target alias,
    /// no FROM, and a mandatory WHERE. It also cannot evaluate a subquery that is correlated with the
    /// target row. A single-table filter with no such subquery is written directly; every other shape
    /// (joins, Take/Skip/OrderBy, navigation filters) selects the target's primary keys in an
    /// uncorrelated <c>IN</c> subquery, which the server evaluates like any SELECT.
    /// </summary>
    protected override Expression VisitUpdate(UpdateExpression updateExpression)
    {
        TableExpression target = updateExpression.Table;
        SelectExpression select = updateExpression.SelectExpression;

        foreach (ColumnValueSetter setter in updateExpression.ColumnValueSetters)
        {
            ColumnScan scan = ColumnScan.Run(setter.Value, target.Alias);
            if (scan.OtherAliasAtTop || scan.TargetInSubquery)
                throw new InvalidOperationException(
                    $"CamusDB cannot translate ExecuteUpdate: the value set on '{setter.Column.Name}' reads another table. " +
                    "CamusDB's UPDATE can only compute a new value from constants, parameters and the row's own columns.");
        }

        Sql.Append("UPDATE ").Append(DelimitTable(target)).AppendLine();
        Sql.Append("SET ");

        _bareColumnAlias = target.Alias;
        try
        {
            for (int i = 0; i < updateExpression.ColumnValueSetters.Count; i++)
            {
                if (i > 0)
                    Sql.Append(",").AppendLine().Append("    ");

                ColumnValueSetter setter = updateExpression.ColumnValueSetters[i];
                Sql.Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(setter.Column.Name)).Append(" = ");
                Visit(setter.Value);
            }
        }
        finally
        {
            _bareColumnAlias = null;
        }

        GenerateMutationFilter(target, select, nameof(EntityFrameworkQueryableExtensions.ExecuteUpdate));
        return updateExpression;
    }

    /// <summary>ExecuteDelete. Same rules as <see cref="VisitUpdate"/>: <c>DELETE FROM t WHERE cond</c>.</summary>
    protected override Expression VisitDelete(DeleteExpression deleteExpression)
    {
        Sql.Append("DELETE FROM ").Append(DelimitTable(deleteExpression.Table));

        GenerateMutationFilter(deleteExpression.Table, deleteExpression.SelectExpression,
            nameof(EntityFrameworkQueryableExtensions.ExecuteDelete));
        return deleteExpression;
    }

    private string DelimitTable(TableExpression table)
        => Dependencies.SqlGenerationHelper.DelimitIdentifier(table.Name, table.Schema);

    private void GenerateMutationFilter(TableExpression target, SelectExpression select, string operation)
    {
        Sql.AppendLine().Append("WHERE ");

        if (IsDirectFilter(target, select))
        {
            if (select.Predicate == null)
            {
                Sql.Append("TRUE");
                return;
            }

            _bareColumnAlias = target.Alias;
            try
            {
                Visit(select.Predicate);
            }
            finally
            {
                _bareColumnAlias = null;
            }

            return;
        }

        if (select.GroupBy.Count > 0 || select.Having != null)
            throw new InvalidOperationException(
                $"CamusDB cannot translate {operation} over a grouped query.");

        IColumnBase keyColumn = SingleKeyColumn(target, operation);
        string key = Dependencies.SqlGenerationHelper.DelimitIdentifier(keyColumn.Name);

        Sql.Append(key).Append(" IN (");

        using (Sql.Indent())
        {
            Sql.AppendLine();
            Sql.Append("SELECT ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(target.Alias!))
                .Append(".")
                .Append(key);

            Sql.AppendLine().Append("FROM ");
            for (int i = 0; i < select.Tables.Count; i++)
            {
                if (i > 0)
                    Sql.AppendLine();

                Visit(select.Tables[i]);
            }

            if (select.Predicate != null)
            {
                Sql.AppendLine().Append("WHERE ");
                Visit(select.Predicate);
            }

            if (select.Orderings.Count > 0)
            {
                Sql.AppendLine().Append("ORDER BY ");
                for (int i = 0; i < select.Orderings.Count; i++)
                {
                    if (i > 0)
                        Sql.Append(", ");

                    VisitOrdering(select.Orderings[i]);
                }
            }

            GenerateLimitOffset(select);
        }

        Sql.AppendLine().Append(")");
    }

    // True when the filter can be written as the statement's own WHERE: one table (the target), nothing
    // that bounds or orders the row set, and no subquery that refers back to the target row.
    private static bool IsDirectFilter(TableExpression target, SelectExpression select)
        => select.Tables.Count == 1
           && ReferenceEquals(select.Tables[0], target)
           && select.Limit == null
           && select.Offset == null
           && select.Orderings.Count == 0
           && select.GroupBy.Count == 0
           && select.Having == null
           && (select.Predicate == null || !ColumnScan.Run(select.Predicate, target.Alias).TargetInSubquery);

    private static IColumnBase SingleKeyColumn(TableExpression target, string operation)
    {
        if (target.Table is ITable { PrimaryKey: { Columns.Count: 1 } primaryKey })
            return primaryKey.Columns[0];

        throw new InvalidOperationException(
            $"CamusDB cannot translate this {operation}: the filter needs a join, a subquery on the target row, " +
            $"or Take/Skip/OrderBy, and table '{target.Name}' has no single-column primary key to select the rows by.");
    }

    /// <summary>
    /// Finds the column references in a SQL tree that matter to an UPDATE/DELETE: a target column inside
    /// a subquery (a correlated subquery, which the server cannot evaluate in UPDATE/DELETE), and a column
    /// of another table outside any subquery (a value that comes from a join).
    /// </summary>
    private sealed class ColumnScan : ExpressionVisitor
    {
        private readonly string? _targetAlias;
        private int _subqueryDepth;

        public bool TargetInSubquery { get; private set; }

        public bool OtherAliasAtTop { get; private set; }

        private ColumnScan(string? targetAlias) => _targetAlias = targetAlias;

        public static ColumnScan Run(Expression expression, string? targetAlias)
        {
            ColumnScan scan = new(targetAlias);
            scan.Visit(expression);
            return scan;
        }

        protected override Expression VisitExtension(Expression node)
        {
            switch (node)
            {
                case ColumnExpression column:
                    if (_subqueryDepth > 0 && column.TableAlias == _targetAlias)
                        TargetInSubquery = true;
                    else if (_subqueryDepth == 0 && column.TableAlias != _targetAlias)
                        OtherAliasAtTop = true;
                    return node;

                case SelectExpression:
                    _subqueryDepth++;
                    try
                    {
                        return base.VisitExtension(node);
                    }
                    finally
                    {
                        _subqueryDepth--;
                    }

                default:
                    return base.VisitExtension(node);
            }
        }
    }
}
