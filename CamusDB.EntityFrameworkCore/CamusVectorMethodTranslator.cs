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
/// Translates the <c>EF.Functions</c> vector helpers onto CamusDB's native vector scalar functions.
///
/// <para>The point of the translation is the ranking query: an <c>OrderBy</c> over a distance plus a
/// <c>Take(k)</c> becomes <c>ORDER BY l2_distance(…) LIMIT k</c>, which the server plans as a bounded
/// top-k rather than a full sort. Nothing here evaluates a distance client-side; a call that reaches
/// this translator with a shape it cannot render returns <see langword="null"/>, and EF Core then
/// reports the query as untranslatable instead of pulling every row into memory.</para>
/// </summary>
public sealed class CamusVectorMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sql;

    public CamusVectorMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sql = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method.DeclaringType != typeof(CamusDbFunctionsExtensions))
            return null;

        // arguments[0] is the DbFunctions receiver; the real arguments follow.
        SqlExpression[] args = arguments.Skip(1).ToArray();

        return method.Name switch
        {
            nameof(CamusDbFunctionsExtensions.L2Distance) => Function("l2_distance", typeof(double), args),
            nameof(CamusDbFunctionsExtensions.CosineDistance) => Function("cosine_distance", typeof(double), args),
            nameof(CamusDbFunctionsExtensions.InnerProduct) => Function("inner_product", typeof(double), args),
            nameof(CamusDbFunctionsExtensions.VectorDims) => Function("vector_dims", typeof(int), args),
            nameof(CamusDbFunctionsExtensions.OctetLength) => Function("octet_length", typeof(int), args),
            _ => null,
        };
    }

    private SqlExpression Function(string name, Type returnType, params SqlExpression[] args)
        => _sql.Function(
            name,
            args,
            nullable: true,
            argumentsPropagateNullability: args.Select(_ => true).ToArray(),
            returnType: returnType);
}
