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
/// Translates the <c>EF.Functions</c> vector helpers onto CamusDB's native vector scalar functions.
///
/// <para>The point of the translation is the ranking query: an <c>OrderBy</c> over a distance plus a
/// <c>Take(k)</c> becomes <c>ORDER BY l2_distance(…) LIMIT k</c>, which the server plans as a bounded
/// top-k rather than a full sort. Nothing here evaluates a distance client-side; a call that reaches
/// this translator with a shape it cannot render returns <see langword="null"/>, and EF Core then
/// reports the query as untranslatable instead of pulling every row into memory.</para>
///
/// <para>The vector arguments share one type mapping, taken from the argument that has one (the column).
/// A <c>float[]</c> query vector then goes to the server as bytes through the converter of the column,
/// not as an <c>ARRAY(FLOAT32)</c>. When no argument has a mapping (two parameters), a <c>float[]</c>
/// argument gets the bytes mapping with <see cref="CamusVectorValueConverter"/>.</para>
/// </summary>
public sealed class CamusVectorMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sql;

    private readonly RelationalTypeMapping _floatVectorMapping;

    public CamusVectorMethodTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
    {
        _sql = sqlExpressionFactory;
        _floatVectorMapping = (RelationalTypeMapping)typeMappingSource.FindMapping(typeof(byte[]))!
            .WithComposedConverter(CamusVectorValueConverter.Instance, CamusVectorValueComparer.Instance);
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
        SqlExpression[] args = WithVectorTypeMapping(arguments.Skip(1).ToArray());

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

    private SqlExpression[] WithVectorTypeMapping(SqlExpression[] args)
    {
        RelationalTypeMapping? mapping = ExpressionExtensions.InferTypeMapping(args);

        return args.Select(arg => arg.TypeMapping is not null ? arg
            : mapping is not null ? _sql.ApplyTypeMapping(arg, mapping)
            : arg.Type == typeof(float[]) ? _sql.ApplyTypeMapping(arg, _floatVectorMapping)
            : arg).ToArray();
    }

    private SqlExpression Function(string name, Type returnType, params SqlExpression[] args)
        => _sql.Function(
            name,
            args,
            nullable: true,
            argumentsPropagateNullability: args.Select(_ => true).ToArray(),
            returnType: returnType);
}
