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
/// Translates <c>Convert.ToInt32</c>, <c>ToInt64</c>, <c>ToDouble</c>, <c>ToDecimal</c> and <c>ToBoolean</c>
/// of a string to <c>CAST(text AS int64 | float64 | numeric | bool)</c>. The usual operand is a text value
/// read out of a document, such as <c>EF.Functions.JsonValueText(e.Payload, "rank")</c>, which can then be
/// grouped, sorted and compared as a number.
///
/// <para>The server parses the text, and it refuses with <c>CADB0400</c> a text that is not a value of the
/// target type: <c>'1.50'</c> is not an <c>int64</c>, and an empty string is not a number. A SQL
/// <c>NULL</c> stays <c>NULL</c>, so cast the result to a nullable type (<c>(long?)Convert.ToInt64(…)</c>)
/// when the operand can be missing.</para>
/// </summary>
public sealed class CamusConvertTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<string, Type> Targets = new()
    {
        [nameof(Convert.ToInt32)] = typeof(int),
        [nameof(Convert.ToInt64)] = typeof(long),
        [nameof(Convert.ToDouble)] = typeof(double),
        [nameof(Convert.ToDecimal)] = typeof(decimal),
        [nameof(Convert.ToBoolean)] = typeof(bool),
    };

    private readonly ISqlExpressionFactory _sql;

    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public CamusConvertTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
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
        if (method.DeclaringType != typeof(Convert)
            || arguments.Count != 1
            || method.GetParameters()[0].ParameterType != typeof(string)
            || !Targets.TryGetValue(method.Name, out Type? target))
            return null;

        return _sql.Convert(_sql.ApplyDefaultTypeMapping(arguments[0]), target, _typeMappingSource.FindMapping(target));
    }
}
