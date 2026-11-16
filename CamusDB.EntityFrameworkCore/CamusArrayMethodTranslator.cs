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
/// Translates <c>Contains</c>, <c>Count</c>, <c>LongCount</c> and <c>Any</c> on an array column that is not
/// a primitive collection: <c>array_contains(tags, x)</c>, <c>cardinality(tags)</c> and
/// <c>cardinality(tags) &gt; 0</c>.
///
/// <para>
/// From EF Core 11, <c>Property(e =&gt; e.Tags)</c> maps an array as one scalar value, and only
/// <c>PrimitiveCollection(e =&gt; e.Tags)</c> makes it a primitive collection. EF Core then does not send these
/// operators to <see cref="CamusQueryableMethodTranslatingExpressionVisitor"/>, so it calls this translator with
/// the column. The column still has <see cref="CamusArrayTypeMapping"/>, and the SQL is the same as for a
/// primitive collection. On EF Core 10, <c>Property</c> makes a primitive collection, and the queryable
/// visitor translates these operators before this translator runs.
/// </para>
/// </summary>
public sealed class CamusArrayMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sql;

    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public CamusArrayMethodTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
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
        // An array has no SQL literal form, so a constant array is not a value the server can read.
        if (instance is not null
            || method.DeclaringType != typeof(Enumerable)
            || !method.IsGenericMethod
            || arguments is not [{ TypeMapping: CamusArrayTypeMapping mapping } array, ..]
            || array is SqlConstantExpression)
            return null;

        return (method.Name, arguments.Count) switch
        {
            (nameof(Enumerable.Contains), 2) => Function(
                "array_contains", typeof(bool), array,
                _sql.ApplyTypeMapping(arguments[1], (RelationalTypeMapping?)mapping.ElementTypeMapping)),
            (nameof(Enumerable.Count), 1) => Function("cardinality", typeof(int), array),
            (nameof(Enumerable.LongCount), 1) => Function("cardinality", typeof(long), array),
            (nameof(Enumerable.Any), 1) => _sql.GreaterThan(
                Function("cardinality", typeof(int), array),
                _sql.Constant(0, _typeMappingSource.FindMapping(typeof(int)))),
            _ => null,
        };
    }

    private SqlExpression Function(string name, Type returnType, params SqlExpression[] arguments)
        => _sql.Function(name, arguments, nullable: true,
            argumentsPropagateNullability: arguments.Select(_ => true).ToArray(),
            returnType, _typeMappingSource.FindMapping(returnType));
}
