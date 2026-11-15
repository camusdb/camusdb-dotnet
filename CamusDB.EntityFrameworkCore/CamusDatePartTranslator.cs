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
/// Translates the parts of a <see cref="DateTime"/> and a <see cref="DateOnly"/>:
/// <list type="bullet">
/// <item><c>Year</c>, <c>Month</c>, <c>Day</c>, <c>Hour</c>, <c>Minute</c>, <c>Second</c>, <c>Millisecond</c>,
/// <c>DayOfWeek</c> and <c>DayOfYear</c> to <c>date_part(unit, x)</c>. The server's units have the .NET
/// values: <c>second</c> is the whole seconds, <c>millisecond</c> is the fraction of the second (0-999),
/// and <c>dow</c> counts from 0 = Sunday, as <see cref="DayOfWeek"/> does.</item>
/// <item><see cref="DateTime.Date"/> to <c>date_trunc('day', x)</c>, and <see cref="DateTime.TimeOfDay"/> to
/// <c>x - date_trunc('day', x)</c>, an interval.</item>
/// <item><see cref="DateOnly.FromDateTime(DateTime)"/> to <c>CAST(x AS date)</c>.</item>
/// </list>
/// A <c>datetime</c> is UTC on the server, so the parts are the UTC parts.
/// </summary>
public sealed class CamusDatePartTranslator : IMemberTranslator, IMethodCallTranslator
{
    private static readonly Dictionary<string, string> Units = new()
    {
        [nameof(DateTime.Year)] = "year",
        [nameof(DateTime.Month)] = "month",
        [nameof(DateTime.Day)] = "day",
        [nameof(DateTime.Hour)] = "hour",
        [nameof(DateTime.Minute)] = "minute",
        [nameof(DateTime.Second)] = "second",
        [nameof(DateTime.Millisecond)] = "millisecond",
        [nameof(DateTime.DayOfWeek)] = "dow",
        [nameof(DateTime.DayOfYear)] = "doy",
    };

    // The parts that a DateOnly has.
    private static readonly HashSet<string> DateOnlyParts =
        [nameof(DateOnly.Year), nameof(DateOnly.Month), nameof(DateOnly.Day), nameof(DateOnly.DayOfWeek), nameof(DateOnly.DayOfYear)];

    private static readonly MethodInfo FromDateTime =
        typeof(DateOnly).GetMethod(nameof(DateOnly.FromDateTime), [typeof(DateTime)])!;

    private readonly ISqlExpressionFactory _sql;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public CamusDatePartTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
    {
        _sql = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null)
            return null;

        if (member.DeclaringType == typeof(DateTime))
        {
            if (member.Name == nameof(DateTime.Date))
                return DayStart(instance);

            if (member.Name == nameof(DateTime.TimeOfDay))
            {
                SqlExpression value = _sql.ApplyDefaultTypeMapping(instance);
                return new SqlBinaryExpression(System.Linq.Expressions.ExpressionType.Subtract, value, DayStart(value),
                    typeof(TimeSpan), _typeMappingSource.FindMapping(typeof(TimeSpan)));
            }

            if (Units.TryGetValue(member.Name, out string? unit))
                return DatePart(unit, instance, returnType);
        }

        if (member.DeclaringType == typeof(DateOnly) && DateOnlyParts.Contains(member.Name))
            return DatePart(Units[member.Name], instance, returnType);

        return null;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method != FromDateTime)
            return null;

        return _sql.Convert(_sql.ApplyDefaultTypeMapping(arguments[0]), typeof(DateOnly), _typeMappingSource.FindMapping(typeof(DateOnly)));
    }

    // date_part gives an int64. DayOfWeek is an enum over int, so it reads as an int too.
    private SqlExpression DatePart(string unit, SqlExpression value, Type returnType)
        => _sql.Function(
            "date_part",
            [_sql.Constant(unit, _typeMappingSource.FindMapping(typeof(string))), _sql.ApplyDefaultTypeMapping(value)],
            nullable: true,
            argumentsPropagateNullability: [false, true],
            returnType,
            _typeMappingSource.FindMapping(typeof(int)));

    private SqlExpression DayStart(SqlExpression value)
    {
        value = _sql.ApplyDefaultTypeMapping(value);
        return _sql.Function(
            "date_trunc",
            [_sql.Constant("day", _typeMappingSource.FindMapping(typeof(string))), value],
            nullable: true,
            argumentsPropagateNullability: [false, true],
            typeof(DateTime),
            value.TypeMapping);
    }
}
