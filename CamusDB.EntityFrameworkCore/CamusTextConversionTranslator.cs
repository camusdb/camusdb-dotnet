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
/// Translates the conversions between a value and its text, and <see cref="Guid.NewGuid"/>:
/// <list type="bullet">
/// <item><c>x.ToString()</c> and <c>Convert.ToString(x)</c> to <c>CAST(x AS string)</c>, for <see cref="int"/>,
/// <see cref="long"/>, <see cref="short"/>, <see cref="double"/>, <see cref="Guid"/>, <see cref="char"/> and
/// <see cref="string"/>. For <see cref="bool"/> the provider writes <c>CASE WHEN x THEN 'True' ELSE 'False' END</c>,
/// because the server's text of a bool is <c>true</c> and .NET writes <c>True</c>.</item>
/// <item><c>int.Parse(s)</c>, <c>long.Parse</c>, <c>short.Parse</c>, <c>double.Parse</c>, <c>decimal.Parse</c>,
/// <c>bool.Parse</c> and <c>Guid.Parse</c> to <c>CAST(s AS type)</c>. The server refuses a text that is not a
/// value of the type with <c>CADB0400</c>, where .NET throws <see cref="FormatException"/>.</item>
/// <item><see cref="Guid.NewGuid"/> to <c>gen_uuid_v4()</c>.</item>
/// </list>
///
/// <para>Not translated on purpose: <see cref="decimal.ToString()"/>, because the server's text of a
/// <c>numeric</c> drops the trailing zeros that .NET keeps (<c>1.50</c> is <c>'1.5'</c>);
/// <see cref="float.ToString()"/>, because a <c>float32</c> can print more digits than .NET; and the
/// date and time types, whose .NET text depends on the culture.</para>
/// </summary>
public sealed class CamusTextConversionTranslator : IMethodCallTranslator
{
    private static readonly HashSet<Type> ToStringTypes =
        [typeof(int), typeof(long), typeof(short), typeof(double), typeof(Guid), typeof(char), typeof(string), typeof(bool)];

    private static readonly HashSet<Type> ParseTypes =
        [typeof(int), typeof(long), typeof(short), typeof(double), typeof(decimal), typeof(bool), typeof(Guid)];

    private static readonly MethodInfo NewGuid = typeof(Guid).GetMethod(nameof(Guid.NewGuid), Type.EmptyTypes)!;

    private readonly ISqlExpressionFactory _sql;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public CamusTextConversionTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
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
        if (method == NewGuid)
            return _sql.Function("gen_uuid_v4", [], nullable: false, argumentsPropagateNullability: [],
                typeof(Guid), _typeMappingSource.FindMapping(typeof(Guid)));

        // x.ToString() with no format. The instance is the value that EF translated.
        if (instance is not null && method.Name == nameof(ToString) && arguments.Count == 0
            && ToStringTypes.Contains(Unwrap(instance.Type)))
            return ToText(instance);

        if (instance is null && method.DeclaringType == typeof(Convert) && method.Name == nameof(Convert.ToString)
            && method.GetParameters() is [{ ParameterType: var source }] && ToStringTypes.Contains(source))
            return ToText(arguments[0]);

        if (instance is null && method.Name == nameof(int.Parse) && method.DeclaringType is { } target
            && ParseTypes.Contains(target) && method.GetParameters() is [{ ParameterType: var text }] && text == typeof(string))
            return _sql.Convert(_sql.ApplyDefaultTypeMapping(arguments[0]), target, _typeMappingSource.FindMapping(target));

        return null;
    }

    private SqlExpression ToText(SqlExpression value)
    {
        RelationalTypeMapping? text = _typeMappingSource.FindMapping(typeof(string));
        value = _sql.ApplyDefaultTypeMapping(value);

        if (value.Type == typeof(string))
            return value;

        if (Unwrap(value.Type) == typeof(bool))
            return _sql.Case(
                [new CaseWhenClause(value, _sql.Constant("True", text))],
                _sql.Constant("False", text));

        return _sql.Convert(value, typeof(string), text);
    }

    private static Type Unwrap(Type type) => Nullable.GetUnderlyingType(type) ?? type;
}
