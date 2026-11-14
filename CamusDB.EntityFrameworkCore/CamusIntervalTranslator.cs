/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Linq.Expressions;
using System.Reflection;
using CamusDB.Client;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Builds the SQL of date and interval arithmetic. The server rules are the PostgreSQL rules:
/// <c>datetime ± interval</c> is a datetime, <c>datetime - datetime</c> is an interval of days and time,
/// <c>date + integer</c> is a date, and <c>interval * number</c> moves a fraction of a month into days
/// and a fraction of a day into the time.
///
/// <para>An amount of a unit is <c>n * INTERVAL '1 unit'</c>. When <c>n</c> is a constant that gives a
/// whole number of the unit (or of microseconds, for a time unit), the product is one interval literal,
/// so <c>AddDays(-7)</c> is <c>x + INTERVAL 'P-7D'</c>.</para>
/// </summary>
internal sealed class CamusIntervalSql
{
    private const long MicrosPerSecond = 1_000_000;

    internal static readonly CamusInterval Year = new(12, 0, 0);
    internal static readonly CamusInterval Month = new(1, 0, 0);
    internal static readonly CamusInterval Week = new(0, 7, 0);
    internal static readonly CamusInterval Day = new(0, 1, 0);
    internal static readonly CamusInterval Hour = new(0, 0, 3600 * MicrosPerSecond);
    internal static readonly CamusInterval Minute = new(0, 0, 60 * MicrosPerSecond);
    internal static readonly CamusInterval Second = new(0, 0, MicrosPerSecond);
    internal static readonly CamusInterval Millisecond = new(0, 0, 1000);
    internal static readonly CamusInterval Microsecond = new(0, 0, 1);

    private readonly ISqlExpressionFactory _sql;

    internal CamusIntervalSql(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sql = sqlExpressionFactory;
    }

    internal static bool IsTemporal(Type type)
    {
        type = Unwrap(type);

        return type == typeof(DateTime) || type == typeof(DateTimeOffset);
    }

    internal static Type Unwrap(Type type) => Nullable.GetUnderlyingType(type) ?? type;

    /// <summary>The interval <c>amount * unit</c>, as one literal when the product is exact.</summary>
    internal SqlExpression Interval(SqlExpression amount, CamusInterval unit)
    {
        if (amount is SqlConstantExpression { Value: { } value } && TryMultiply(value, unit, out CamusInterval product))
            return Literal(product);

        return Binary(ExpressionType.Multiply, amount, Literal(unit), typeof(TimeSpan), CamusIntervalTypeMapping.Instance);
    }

    /// <summary><c>instance + amount * unit</c> for a datetime instance.</summary>
    internal SqlExpression AddUnits(SqlExpression instance, SqlExpression amount, CamusInterval unit, Type resultType)
        => AddInterval(instance, Interval(amount, unit), resultType);

    /// <summary><c>instance ± interval</c>, a datetime with the type mapping of the instance.</summary>
    internal SqlExpression AddInterval(SqlExpression instance, SqlExpression interval, Type resultType, bool subtract = false)
    {
        instance = _sql.ApplyDefaultTypeMapping(instance);

        return Binary(subtract ? ExpressionType.Subtract : ExpressionType.Add, instance, interval, Unwrap(resultType), instance.TypeMapping!);
    }

    /// <summary><c>left - right</c> for two datetimes: an interval of days and time.</summary>
    internal SqlExpression Difference(SqlExpression left, SqlExpression right)
        => Binary(ExpressionType.Subtract, left, right, typeof(TimeSpan), CamusIntervalTypeMapping.Instance);

    /// <summary>
    /// A binary expression whose type mapping is set here. The factory would infer one mapping for both
    /// operands, which is wrong when they have different types, such as a datetime and an interval.
    /// </summary>
    internal SqlExpression Binary(ExpressionType op, SqlExpression left, SqlExpression right, Type type, RelationalTypeMapping typeMapping)
        => new SqlBinaryExpression(op, _sql.ApplyDefaultTypeMapping(left), _sql.ApplyDefaultTypeMapping(right), type, typeMapping);

    internal SqlExpression Literal(CamusInterval value)
        => new SqlConstantExpression(value, typeof(TimeSpan), CamusIntervalTypeMapping.Instance);

    // The product of a constant amount and a unit, when it is a whole number of every part. A fraction of
    // a month or a day goes to the server, which applies the PostgreSQL rule to it.
    private static bool TryMultiply(object value, CamusInterval unit, out CamusInterval product)
    {
        product = default;

        decimal amount;

        try
        {
            amount = value switch
            {
                int i => i,
                long l => l,
                short s => s,
                double d when double.IsFinite(d) => (decimal)d,
                float f when float.IsFinite(f) => (decimal)f,
                _ => throw new OverflowException(),
            };

            decimal months = amount * unit.Months;
            decimal days = amount * unit.Days;
            decimal micros = amount * unit.Microseconds;

            if (decimal.Truncate(months) != months || decimal.Truncate(days) != days || decimal.Truncate(micros) != micros)
                return false;

            product = new CamusInterval(decimal.ToInt32(months), decimal.ToInt32(days), decimal.ToInt64(micros));
            return true;
        }
        catch (OverflowException)
        {
            return false;
        }
    }
}

/// <summary>
/// Translates the date and the interval methods of <see cref="DateTime"/>, <see cref="DateTimeOffset"/>,
/// <see cref="DateOnly"/> and <see cref="TimeSpan"/> to interval arithmetic.
///
/// <list type="bullet">
/// <item><c>AddYears</c>, <c>AddMonths</c>, <c>AddDays</c>, <c>AddHours</c>, <c>AddMinutes</c>,
/// <c>AddSeconds</c>, <c>AddMilliseconds</c> and <c>AddMicroseconds</c> become <c>x + n * INTERVAL '1 unit'</c>.
/// A month added to the 31st moves back to the last day of the month, as in .NET.</item>
/// <item><c>Add(TimeSpan)</c> and <c>Subtract(TimeSpan)</c> become <c>x ± interval</c>, and
/// <c>Subtract(DateTime)</c> becomes <c>x - y</c>, an interval.</item>
/// <item><c>DateOnly.AddDays(n)</c> is <c>d + n</c>, a date. <c>DateOnly.AddMonths</c> and <c>AddYears</c>
/// cast the datetime back to a date.</item>
/// <item><c>TimeSpan.FromDays(x)</c> and the other one-argument <c>From…</c> methods become
/// <c>x * INTERVAL '1 unit'</c>. <c>TimeSpan.Add</c>, <c>Subtract</c>, <c>Negate</c>, <c>Multiply</c> and
/// <c>Divide(double)</c> become interval arithmetic.</item>
/// </list>
///
/// <para>A <see cref="DateTime"/> value on the server is UTC, so a day is always 24 hours.</para>
/// </summary>
public sealed class CamusIntervalMethodTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<string, CamusInterval> AddUnits = new(StringComparer.Ordinal)
    {
        ["AddYears"] = CamusIntervalSql.Year,
        ["AddMonths"] = CamusIntervalSql.Month,
        ["AddDays"] = CamusIntervalSql.Day,
        ["AddHours"] = CamusIntervalSql.Hour,
        ["AddMinutes"] = CamusIntervalSql.Minute,
        ["AddSeconds"] = CamusIntervalSql.Second,
        ["AddMilliseconds"] = CamusIntervalSql.Millisecond,
        ["AddMicroseconds"] = CamusIntervalSql.Microsecond,
    };

    private static readonly Dictionary<string, CamusInterval> FromUnits = new(StringComparer.Ordinal)
    {
        [nameof(TimeSpan.FromDays)] = CamusIntervalSql.Day,
        [nameof(TimeSpan.FromHours)] = CamusIntervalSql.Hour,
        [nameof(TimeSpan.FromMinutes)] = CamusIntervalSql.Minute,
        [nameof(TimeSpan.FromSeconds)] = CamusIntervalSql.Second,
        [nameof(TimeSpan.FromMilliseconds)] = CamusIntervalSql.Millisecond,
        [nameof(TimeSpan.FromMicroseconds)] = CamusIntervalSql.Microsecond,
    };

    private readonly ISqlExpressionFactory _sql;
    private readonly IRelationalTypeMappingSource _typeMappingSource;
    private readonly CamusIntervalSql _interval;

    public CamusIntervalMethodTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
    {
        _sql = sqlExpressionFactory;
        _typeMappingSource = typeMappingSource;
        _interval = new CamusIntervalSql(sqlExpressionFactory);
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        Type? declaringType = method.DeclaringType;

        if (declaringType == typeof(TimeSpan))
            return TranslateTimeSpan(instance, method, arguments);

        if (instance is null || arguments.Count != 1)
            return null;

        if (declaringType == typeof(DateOnly))
            return TranslateDateOnly(instance, method, arguments[0]);

        if (declaringType != typeof(DateTime) && declaringType != typeof(DateTimeOffset))
            return null;

        if (AddUnits.TryGetValue(method.Name, out CamusInterval unit) && IsNumber(method.GetParameters()[0].ParameterType))
            return _interval.AddUnits(instance, arguments[0], unit, method.ReturnType);

        Type parameterType = method.GetParameters()[0].ParameterType;

        return method.Name switch
        {
            nameof(DateTime.Add) when parameterType == typeof(TimeSpan)
                => _interval.AddInterval(instance, arguments[0], method.ReturnType),
            nameof(DateTime.Subtract) when parameterType == typeof(TimeSpan)
                => _interval.AddInterval(instance, arguments[0], method.ReturnType, subtract: true),
            nameof(DateTime.Subtract) when parameterType == declaringType
                => _interval.Difference(instance, arguments[0]),
            _ => null,
        };
    }

    private SqlExpression? TranslateDateOnly(SqlExpression instance, MethodInfo method, SqlExpression amount)
    {
        if (method.GetParameters()[0].ParameterType != typeof(int))
            return null;

        instance = _sql.ApplyDefaultTypeMapping(instance);

        switch (method.Name)
        {
            // date + integer is a date on the server.
            case nameof(DateOnly.AddDays):
                return _interval.Binary(ExpressionType.Add, instance, amount, typeof(DateOnly), instance.TypeMapping!);

            // date + interval is a datetime at midnight UTC, so the result is cast back to a date.
            case nameof(DateOnly.AddMonths) or nameof(DateOnly.AddYears):
                RelationalTypeMapping datetime = _typeMappingSource.FindMapping(typeof(DateTime))!;
                CamusInterval unit = method.Name == nameof(DateOnly.AddMonths) ? CamusIntervalSql.Month : CamusIntervalSql.Year;
                SqlExpression moved = _interval.Binary(ExpressionType.Add, instance, _interval.Interval(amount, unit), typeof(DateTime), datetime);
                return _sql.Convert(moved, typeof(DateOnly), instance.TypeMapping);

            default:
                return null;
        }
    }

    private SqlExpression? TranslateTimeSpan(SqlExpression? instance, MethodInfo method, IReadOnlyList<SqlExpression> arguments)
    {
        ParameterInfo[] parameters = method.GetParameters();

        if (instance is null)
        {
            return parameters.Length == 1 && IsNumber(parameters[0].ParameterType) && FromUnits.TryGetValue(method.Name, out CamusInterval unit)
                ? _interval.Interval(arguments[0], unit)
                : null;
        }

        if (method.Name == nameof(TimeSpan.Negate) && parameters.Length == 0)
            return _sql.Negate(_sql.ApplyTypeMapping(instance, CamusIntervalTypeMapping.Instance));

        if (parameters.Length != 1)
            return null;

        Type parameterType = parameters[0].ParameterType;
        ExpressionType? op = method.Name switch
        {
            nameof(TimeSpan.Add) when parameterType == typeof(TimeSpan) => ExpressionType.Add,
            nameof(TimeSpan.Subtract) when parameterType == typeof(TimeSpan) => ExpressionType.Subtract,
            nameof(TimeSpan.Multiply) when parameterType == typeof(double) => ExpressionType.Multiply,
            nameof(TimeSpan.Divide) when parameterType == typeof(double) => ExpressionType.Divide,
            _ => null,
        };

        return op is { } binary
            ? _interval.Binary(binary, instance, arguments[0], typeof(TimeSpan), CamusIntervalTypeMapping.Instance)
            : null;
    }

    private static bool IsNumber(Type type)
        => type == typeof(int) || type == typeof(long) || type == typeof(double);
}

/// <summary>
/// Translates <see cref="DateTime.UtcNow"/> and <see cref="DateTimeOffset.UtcNow"/> to <c>now()</c>, the
/// start of the transaction in UTC. <see cref="DateTime.Now"/> and <see cref="DateTime.Today"/> are local
/// times, which the server does not know, so they are not translated.
/// </summary>
public sealed class CamusDateTimeMemberTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sql;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public CamusDateTimeMemberTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
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
        if (instance is not null || member.Name != nameof(DateTime.UtcNow))
            return null;

        if (member.DeclaringType != typeof(DateTime) && member.DeclaringType != typeof(DateTimeOffset))
            return null;

        return _sql.Function(
            "now",
            [],
            nullable: false,
            argumentsPropagateNullability: [],
            returnType,
            _typeMappingSource.FindMapping(returnType));
    }
}
