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
/// Translates the <c>EF.Functions</c> JSON helpers onto CamusDB's <c>jsonb_*</c> functions, the function
/// forms of <c>@&gt;</c>, <c>&lt;@</c>, <c>?</c>, <c>?|</c> and <c>?&amp;</c>. The server plans a function
/// form as it plans its operator, so a GIN index serves both.
///
/// <para>The document arguments share one type mapping. An argument without one (a parameter or a
/// constant) takes the <c>jsonb</c> mapping of the other document argument, or the <c>jsonb</c> mapping of
/// a string when the other argument has none. A parameter therefore binds as <c>jsonb</c>, and a constant
/// is the typed literal <c>JSONB '…'</c>. A document argument that is a plain string column keeps its
/// string mapping: the server reads its text as a document.</para>
/// </summary>
public sealed class CamusJsonMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sql;

    private readonly RelationalTypeMapping _jsonbTextMapping;

    public CamusJsonMethodTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
    {
        _sql = sqlExpressionFactory;
        _jsonbTextMapping = typeMappingSource.FindMapping(typeof(string), "jsonb")!;
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
        return method.Name switch
        {
            nameof(CamusDbFunctionsExtensions.JsonContains)
                => Function("jsonb_contains", typeof(bool), Documents(arguments[1], arguments[2])),
            nameof(CamusDbFunctionsExtensions.JsonContained)
                => Function("jsonb_contained", typeof(bool), Documents(arguments[1], arguments[2])),
            nameof(CamusDbFunctionsExtensions.JsonExists)
                => Function("jsonb_exists", typeof(bool), Document(arguments[1]), _sql.ApplyDefaultTypeMapping(arguments[2])),
            nameof(CamusDbFunctionsExtensions.JsonExistAny)
                => Function("jsonb_exists_any", typeof(bool), Document(arguments[1]), _sql.ApplyDefaultTypeMapping(arguments[2])),
            nameof(CamusDbFunctionsExtensions.JsonExistAll)
                => Function("jsonb_exists_all", typeof(bool), Document(arguments[1]), _sql.ApplyDefaultTypeMapping(arguments[2])),
            nameof(CamusDbFunctionsExtensions.JsonTypeof)
                => Function("jsonb_typeof", typeof(string), Document(arguments[1])),
            _ => null,
        };
    }

    private SqlExpression[] Documents(SqlExpression first, SqlExpression second)
    {
        first = Unbox(first);
        second = Unbox(second);

        RelationalTypeMapping mapping = ExpressionExtensions.InferTypeMapping(first, second) is CamusJsonbTypeMapping jsonb
            ? jsonb
            : _jsonbTextMapping;

        return [WithMapping(first, mapping), WithMapping(second, mapping)];
    }

    private SqlExpression Document(SqlExpression document) => WithMapping(Unbox(document), _jsonbTextMapping);

    // A document argument is typed object. EF Core drops the conversion of a reference type to object,
    // but it keeps the box of a struct (a JsonElement column) as CAST(col AS jsonb). The server reads that
    // cast as an expression, not as the column, so a GIN index could not serve the predicate.
    private static SqlExpression Unbox(SqlExpression argument)
        => argument is SqlUnaryExpression { OperatorType: System.Linq.Expressions.ExpressionType.Convert } convert
           && convert.Type == typeof(object)
            ? convert.Operand
            : argument;

    private SqlExpression WithMapping(SqlExpression argument, RelationalTypeMapping mapping)
        => argument.TypeMapping is not null ? argument : _sql.ApplyTypeMapping(argument, mapping);

    private SqlExpression Function(string name, Type returnType, params SqlExpression[] args)
        => _sql.Function(
            name,
            args,
            nullable: true,
            argumentsPropagateNullability: args.Select(_ => true).ToArray(),
            returnType: returnType);
}
