using CamusDB.Client;
using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

public class CamusQuerySqlGenerator : QuerySqlGenerator
{
    // The alias of the ExecuteUpdate/ExecuteDelete target while its columns are written bare. The plain
    // UPDATE and DELETE forms have no alias on the target table, and the server does not resolve a column
    // qualified by the table name either, so a target column is addressable only by its bare name there.
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

        // An OFFSET with no LIMIT stands alone, as in PostgreSQL. The server refuses a negative LIMIT
        // (CADB0400), so the SQLite form LIMIT -1 OFFSET n is not available.
        if (selectExpression.Offset != null)
        {
            if (selectExpression.Limit == null)
                Sql.AppendLine().Append("OFFSET ");
            else
                Sql.Append(" OFFSET ");

            Visit(selectExpression.Offset);
        }
    }

    // The server does not read a string parameter as an interval by itself, so an interval parameter is
    // cast. Its value is the ISO-8601 text that the command binds for a TimeSpan.
    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        if (sqlParameterExpression.TypeMapping is not CamusIntervalTypeMapping)
            return base.VisitSqlParameter(sqlParameterExpression);

        Sql.Append("CAST(");
        base.VisitSqlParameter(sqlParameterExpression);
        Sql.Append(" AS ").Append(CamusIntervalTypeMapping.StoreTypeName).Append(")");

        return sqlParameterExpression;
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

    // The element table of an array column (see CamusQueryableMethodTranslatingExpressionVisitor). A query
    // that still reads it composes more than Contains, Count or Any over the elements, and the server has
    // no function that turns an array into rows.
    protected override Expression VisitTableValuedFunction(TableValuedFunctionExpression tableValuedFunctionExpression)
    {
        if (tableValuedFunctionExpression.Name == CamusQueryableMethodTranslatingExpressionVisitor.ArrayElementsFunction)
            throw new InvalidOperationException(
                "CamusDB cannot translate this query over the elements of an array column. The provider translates " +
                "Contains, Count, Any and Length on an array column, because the server reads an array only as a whole. " +
                "Load the array and query its elements on the client.");

        return base.VisitTableValuedFunction(tableValuedFunctionExpression);
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

    // CamusDB joins strings with ||, as PostgreSQL and SQLite do, and refuses + for two strings. EF writes
    // a string concatenation (a + b, string.Concat) as an Add whose type is string. The NULL rule of || (a
    // NULL operand gives NULL) does not leak: EF already wraps each nullable operand in COALESCE(x, '').
    protected override string GetOperator(SqlBinaryExpression binaryExpression)
        => binaryExpression.OperatorType == ExpressionType.Add && binaryExpression.Type == typeof(string)
            ? " || "
            : base.GetOperator(binaryExpression);

    // A function mapped with HasDbFunction, for example a C# extension function that the server loads.
    // EF delimits the name of a non-built-in function and prefixes its schema, but the server reads a
    // function name only as a plain identifier and has no schemas, so the name is written bare.
    protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
    {
        if (sqlFunctionExpression.IsBuiltIn)
            return base.VisitSqlFunction(sqlFunctionExpression);

        string name = sqlFunctionExpression.Name;
        if (!CamusSqlSyntax.IsPlainIdentifier(name))
            throw new InvalidOperationException(
                $"CamusDB cannot call the function '{name}': a function name must be a plain identifier " +
                "(a letter or '_', then letters, digits or '_'), because the server does not read a delimited function name.");

        Sql.Append(name);

        if (!sqlFunctionExpression.IsNiladic)
        {
            Sql.Append("(");
            IReadOnlyList<SqlExpression> arguments = sqlFunctionExpression.Arguments ?? [];
            for (int i = 0; i < arguments.Count; i++)
            {
                if (i > 0)
                    Sql.Append(", ");
                Visit(arguments[i]);
            }
            Sql.Append(")");
        }

        return sqlFunctionExpression;
    }

    private static InvalidOperationException ApplyNotSupported()
        => new(
            "CamusDB cannot translate this query: it needs a lateral join (CROSS APPLY / OUTER APPLY), which CamusDB does not support. " +
            "EF Core writes one when the inner sequence of a SelectMany or a join refers to the outer row outside an equality key. " +
            "Make the correlation an equality join key, or issue separate queries.");

    /// <summary>
    /// ExecuteUpdate. The provider writes one of two forms.
    ///
    /// <para><b>The plain form</b>, <c>UPDATE t SET c = v[, ...] WHERE cond</c>, with no target alias and
    /// bare target columns. A single-table filter is the statement's own WHERE. Every other filter (joins,
    /// Take/Skip/OrderBy, a subquery on the target row) selects the target's primary keys in an
    /// uncorrelated <c>IN</c> subquery. The <c>IN</c> list holds each key once, so a target row that a
    /// join matches two times changes once. Every CamusDB server version runs this form.</para>
    ///
    /// <para><b>The alias form</b>, <c>UPDATE t AS a SET c = v FROM others WHERE cond</c>, the PostgreSQL
    /// form (server commit 3d8247a2). The provider writes it only when the plain form cannot express the
    /// statement: a new value that reads another table of the query (a join column or a subquery on the
    /// target row), or a filter that needs the <c>IN</c> subquery on a table with no single-column
    /// primary key. The server refuses this form (<c>CADB0400</c>) when one target row matches two or
    /// more rows of the other tables.</para>
    /// </summary>
    protected override Expression VisitUpdate(UpdateExpression updateExpression)
    {
        TableExpression target = updateExpression.Table;
        SelectExpression select = updateExpression.SelectExpression;

        bool valueReadsOtherTable = false;
        foreach (ColumnValueSetter setter in updateExpression.ColumnValueSetters)
        {
            ColumnScan scan = ColumnScan.Run(setter.Value, target.Alias);
            valueReadsOtherTable |= scan.OtherAliasAtTop || scan.TargetInSubquery || scan.OtherAliasInSubquery;
        }

        if (valueReadsOtherTable || NeedsAliasForm(target, select))
        {
            GenerateAliasedUpdate(updateExpression);
            return updateExpression;
        }

        Sql.Append("UPDATE ").Append(DelimitTable(target)).AppendLine();
        Sql.Append("SET ");

        _bareColumnAlias = target.Alias;
        try
        {
            GenerateSetters(updateExpression);
        }
        finally
        {
            _bareColumnAlias = null;
        }

        GenerateMutationFilter(target, select, nameof(EntityFrameworkQueryableExtensions.ExecuteUpdate));
        return updateExpression;
    }

    /// <summary>ExecuteDelete. Same rules as <see cref="VisitUpdate"/>: <c>DELETE FROM t WHERE cond</c>, or
    /// <c>DELETE FROM t AS a [USING others] WHERE cond</c> when the filter needs the <c>IN</c> subquery and
    /// the table has no single-column primary key. A DELETE that matches a row two times deletes it once.</summary>
    protected override Expression VisitDelete(DeleteExpression deleteExpression)
    {
        TableExpression target = deleteExpression.Table;
        SelectExpression select = deleteExpression.SelectExpression;

        if (NeedsAliasForm(target, select))
        {
            Sql.Append("DELETE FROM ").Append(DelimitTable(target)).Append(" AS ")
                .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(target.Alias!));

            GenerateAliasedSources(target, select, "USING", nameof(EntityFrameworkQueryableExtensions.ExecuteDelete));
            return deleteExpression;
        }

        Sql.Append("DELETE FROM ").Append(DelimitTable(target));

        GenerateMutationFilter(target, select, nameof(EntityFrameworkQueryableExtensions.ExecuteDelete));
        return deleteExpression;
    }

    // The plain form needs a single-column primary key for every filter that it cannot write directly.
    private static bool NeedsAliasForm(TableExpression target, SelectExpression select)
        => !IsDirectFilter(target, select) && !HasSingleKeyColumn(target);

    private void GenerateSetters(UpdateExpression updateExpression)
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

    private void GenerateAliasedUpdate(UpdateExpression updateExpression)
    {
        TableExpression target = updateExpression.Table;

        Sql.Append("UPDATE ").Append(DelimitTable(target)).Append(" AS ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(target.Alias!)).AppendLine();

        // The target of SET is a bare column: the PostgreSQL form does not qualify it. A value reads the
        // target through its alias.
        Sql.Append("SET ");
        GenerateSetters(updateExpression);

        GenerateAliasedSources(target, updateExpression.SelectExpression, "FROM", nameof(EntityFrameworkQueryableExtensions.ExecuteUpdate));
    }

    /// <summary>
    /// Writes the relation list (<c>FROM</c> of an UPDATE, <c>USING</c> of a DELETE) and the WHERE of the
    /// alias form. Each table of the query other than the target goes into the list. The condition of
    /// each inner join goes into the WHERE with the query's filter, because a join condition in the list
    /// cannot read the target. A query with an outer join, or one that orders, bounds or groups its rows,
    /// has no such form and is refused.
    /// </summary>
    private void GenerateAliasedSources(TableExpression target, SelectExpression select, string listKeyword, string operation)
    {
        if (select.Limit != null || select.Offset != null || select.Orderings.Count > 0
            || select.GroupBy.Count > 0 || select.Having != null || select.IsDistinct)
            throw new InvalidOperationException(
                $"CamusDB cannot translate this {operation}: the query orders, bounds or groups its rows, and table " +
                $"'{target.Name}' has no single-column primary key to select the rows by.");

        List<TableExpressionBase> sources = [];
        SqlExpression? condition = null;

        foreach (TableExpressionBase table in select.Tables)
        {
            if (IsTarget(table, target))
                continue;

            switch (table)
            {
                case InnerJoinExpression innerJoin:
                    if (!IsTarget(innerJoin.Table, target))
                        sources.Add(innerJoin.Table);
                    condition = And(condition, innerJoin.JoinPredicate);
                    break;

                case CrossJoinExpression crossJoin:
                    if (!IsTarget(crossJoin.Table, target))
                        sources.Add(crossJoin.Table);
                    break;

                case JoinExpressionBase:
                    throw new InvalidOperationException(
                        $"CamusDB cannot translate this {operation}: it needs an outer join in the {listKeyword} list of the statement, " +
                        "and CamusDB has no form for it. Use an inner join or a subquery in the filter.");

                default:
                    sources.Add(table);
                    break;
            }
        }

        if (sources.Count > 0)
        {
            Sql.AppendLine().Append(listKeyword).Append(" ");
            for (int i = 0; i < sources.Count; i++)
            {
                if (i > 0)
                    Sql.Append(", ");

                Visit(sources[i]);
            }
        }

        if (select.Predicate != null)
            condition = And(condition, select.Predicate);

        Sql.AppendLine().Append("WHERE ");
        if (condition == null)
            Sql.Append("TRUE");
        else
            Visit(condition);
    }

    // EF Core can put a new TableExpression for the target into the query's tables (ExecuteDelete does
    // when it maps the entity to its table), so the target is found by its alias, which is unique in
    // the query.
    private static bool IsTarget(TableExpressionBase table, TableExpression target)
        => table is TableExpression { Alias: { } alias } && alias == target.Alias;

    private static SqlExpression And(SqlExpression? left, SqlExpression right)
        => left == null
            ? right
            : new SqlBinaryExpression(ExpressionType.AndAlso, left, right, typeof(bool), right.TypeMapping);

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
           && IsTarget(select.Tables[0], target)
           && select.Limit == null
           && select.Offset == null
           && select.Orderings.Count == 0
           && select.GroupBy.Count == 0
           && select.Having == null
           && (select.Predicate == null || !ColumnScan.Run(select.Predicate, target.Alias).TargetInSubquery);

    private static bool HasSingleKeyColumn(TableExpression target)
        => target.Table is ITable { PrimaryKey.Columns.Count: 1 };

    private static IColumnBase SingleKeyColumn(TableExpression target, string operation)
    {
        if (target.Table is ITable { PrimaryKey: { Columns.Count: 1 } primaryKey })
            return primaryKey.Columns[0];

        throw new InvalidOperationException(
            $"CamusDB cannot translate this {operation}: the filter needs a join, a subquery on the target row, " +
            $"or Take/Skip/OrderBy, and table '{target.Name}' has no single-column primary key to select the rows by.");
    }

    /// <summary>
    /// Finds the column references in a SQL tree that matter to an UPDATE/DELETE:
    /// <list type="bullet">
    /// <item><see cref="TargetInSubquery"/>: a subquery reads the target row (a correlated subquery).</item>
    /// <item><see cref="OtherAliasAtTop"/>: a column of another table of the query, outside any subquery (a
    /// value that comes from a join).</item>
    /// <item><see cref="OtherAliasInSubquery"/>: a subquery reads a table of the enclosing query that is not
    /// the target, such as the derived table that EF Core joins for Take/Skip.</item>
    /// </list>
    /// A column inside a subquery is an outer reference when no table of that subquery, or of a subquery
    /// around it, has its alias.
    /// </summary>
    private sealed class ColumnScan : ExpressionVisitor
    {
        private readonly string? _targetAlias;
        private readonly List<HashSet<string>> _subqueryAliases = [];

        public bool TargetInSubquery { get; private set; }

        public bool OtherAliasAtTop { get; private set; }

        public bool OtherAliasInSubquery { get; private set; }

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
                    if (_subqueryAliases.Count == 0)
                    {
                        if (column.TableAlias != _targetAlias)
                            OtherAliasAtTop = true;
                    }
                    else if (!_subqueryAliases.Exists(aliases => aliases.Contains(column.TableAlias)))
                    {
                        if (column.TableAlias == _targetAlias)
                            TargetInSubquery = true;
                        else
                            OtherAliasInSubquery = true;
                    }
                    return node;

                case SelectExpression select:
                    HashSet<string> aliases = new(StringComparer.Ordinal);
                    foreach (TableExpressionBase table in select.Tables)
                    {
                        if (table.UnwrapJoin().Alias is { } alias)
                            aliases.Add(alias);
                    }

                    _subqueryAliases.Add(aliases);
                    try
                    {
                        return base.VisitExtension(node);
                    }
                    finally
                    {
                        _subqueryAliases.RemoveAt(_subqueryAliases.Count - 1);
                    }

                default:
                    return base.VisitExtension(node);
            }
        }
    }
}
