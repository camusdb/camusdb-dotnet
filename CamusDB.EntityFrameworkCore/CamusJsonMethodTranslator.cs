/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Reflection;
using System.Text.Json;
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
///
/// <para>The value readers translate to the function forms of <c>-&gt;</c>, <c>-&gt;&gt;</c>, <c>#&gt;</c>
/// and <c>#&gt;&gt;</c>. Their key keeps a plain string (or integer) mapping, and a text result keeps the
/// string mapping, so in <c>jsonb_object_field_text(col, 'k') = 'v'</c> both constants are strings: that is
/// the one extraction shape a GIN index serves.</para>
/// </summary>
public sealed class CamusJsonMethodTranslator : IMethodCallTranslator
{
    private readonly ISqlExpressionFactory _sql;

    private readonly RelationalTypeMapping _jsonbTextMapping;

    private readonly RelationalTypeMapping _jsonbElementMapping;

    private readonly RelationalTypeMapping _stringMapping;

    public CamusJsonMethodTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
    {
        _sql = sqlExpressionFactory;
        _jsonbTextMapping = typeMappingSource.FindMapping(typeof(string), "jsonb")!;
        _jsonbElementMapping = typeMappingSource.FindMapping(typeof(JsonElement))!;
        _stringMapping = typeMappingSource.FindMapping(typeof(string))!;
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
            nameof(CamusDbFunctionsExtensions.JsonValueText)
                => Function("jsonb_object_field_text", typeof(string), Document(arguments[1]), _sql.ApplyDefaultTypeMapping(arguments[2])),
            nameof(CamusDbFunctionsExtensions.JsonValue)
                => DocumentFunction("jsonb_object_field", Document(arguments[1]), _sql.ApplyDefaultTypeMapping(arguments[2])),
            nameof(CamusDbFunctionsExtensions.JsonPathText)
                => Path(arguments[1], arguments[2], text: true),
            nameof(CamusDbFunctionsExtensions.JsonPath)
                => Path(arguments[1], arguments[2], text: false),
            _ => null,
        };
    }

    // A path of constants becomes one argument per key of jsonb_extract_path[_text], so a key that holds a
    // comma or a brace stays one key. A path in a variable is one ARRAY parameter of jsonb_path_get[_text],
    // which reads an array of strings as its path. An empty constant path is the text array '{}', the whole
    // document: jsonb_extract_path takes at least one key.
    private SqlExpression Path(SqlExpression document, SqlExpression path, bool text)
    {
        SqlExpression doc = Document(document);

        if (path is SqlConstantExpression { Value: string[] keys })
        {
            if (keys.Length == 0)
                return PathFunction(text ? "jsonb_path_get_text" : "jsonb_path_get", text, doc, _sql.Constant("{}", _stringMapping));

            return ExtractPath(doc, keys.Select(k => (SqlExpression)_sql.Constant(k, typeof(string), _stringMapping)).ToList(), text);
        }

        return PathFunction(text ? "jsonb_path_get_text" : "jsonb_path_get", text, doc, _sql.ApplyDefaultTypeMapping(path));
    }

    /// <summary>
    /// <c>jsonb_extract_path[_text](document, key1, key2, …)</c>, one argument per key. The SQL translating
    /// visitor calls it for a path written in place with a variable among its keys, which EF Core leaves as
    /// an array expression the relational base cannot translate. Each key binds as a string.
    /// </summary>
    internal SqlExpression ExtractPath(SqlExpression document, IReadOnlyList<SqlExpression> keys, bool text)
    {
        SqlExpression[] args = new SqlExpression[keys.Count + 1];
        args[0] = Document(document);

        for (int i = 0; i < keys.Count; i++)
            args[i + 1] = WithMapping(keys[i], _stringMapping);

        return PathFunction(text ? "jsonb_extract_path_text" : "jsonb_extract_path", text, args);
    }

    private SqlExpression PathFunction(string name, bool text, params SqlExpression[] args)
        => text ? Function(name, typeof(string), args) : DocumentFunction(name, args);

    // A document result is typed jsonb, so it can be the document argument of another JSON method, and a
    // projection of it reads back through the JsonElement mapping.
    private SqlExpression DocumentFunction(string name, params SqlExpression[] args)
        => _sql.Function(
            name,
            args,
            nullable: true,
            argumentsPropagateNullability: args.Select(_ => true).ToArray(),
            returnType: typeof(JsonElement?),
            typeMapping: _jsonbElementMapping);

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
