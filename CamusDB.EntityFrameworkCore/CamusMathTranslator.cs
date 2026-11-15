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
/// Translates the <see cref="Math"/> methods onto the server's numeric functions: <c>abs</c>, <c>ceil</c>,
/// <c>floor</c>, <c>trunc</c>, <c>round</c>, <c>sign</c>, <c>sqrt</c>, <c>pow</c>, <c>exp</c>, <c>ln</c>,
/// <c>log</c>, <c>log10</c>, and the trigonometric functions <c>sin</c>, <c>cos</c>, <c>tan</c>,
/// <c>asin</c>, <c>acos</c>, <c>atan</c>, <c>atan2</c>, <c>radians</c> and <c>degrees</c>. It also writes
/// <c>EF.Functions.Random()</c> as <c>random()</c>. <see cref="Math.Max(double, double)"/> and
/// <see cref="Math.Min(double, double)"/> are in <see cref="CamusSqlTranslatingExpressionVisitor"/>.
///
/// <para>The overloads are those whose server result has the .NET result type. <c>abs</c>, <c>ceil</c>,
/// <c>floor</c>, <c>trunc</c> and <c>round</c> keep an <c>int64</c> or a <c>numeric</c> argument type and
/// give <c>float64</c> for the other types, so they translate for the integer types (which the provider
/// stores as <c>int64</c>), <see cref="decimal"/> and <see cref="double"/>. The other functions give
/// <c>float64</c>, so they translate for <see cref="double"/>. <c>sign</c> gives <c>numeric</c> for a
/// <c>numeric</c> argument, so <see cref="Math.Sign(decimal)"/> does not translate.</para>
///
/// <para>The server's <c>round</c> rounds a midpoint away from zero. <see cref="Math.Round(double)"/>
/// rounds a midpoint to the even number, so only the overloads that take
/// <see cref="MidpointRounding.AwayFromZero"/> as a constant translate.</para>
///
/// <para>The server's one-argument <c>log</c> is the base-10 logarithm, as in PostgreSQL, so
/// <see cref="Math.Log(double)"/> is <c>ln</c> and <see cref="Math.Log(double, double)"/> is
/// <c>log(base, x)</c>.</para>
/// </summary>
public sealed class CamusMathTranslator : IMethodCallTranslator
{
    private static readonly Dictionary<MethodInfo, string> SameTypeFunctions = new()
    {
        [Method(nameof(Math.Abs), typeof(double))] = "abs",
        [Method(nameof(Math.Abs), typeof(decimal))] = "abs",
        [Method(nameof(Math.Abs), typeof(long))] = "abs",
        [Method(nameof(Math.Abs), typeof(int))] = "abs",
        [Method(nameof(Math.Abs), typeof(short))] = "abs",
        [Method(nameof(Math.Ceiling), typeof(double))] = "ceil",
        [Method(nameof(Math.Ceiling), typeof(decimal))] = "ceil",
        [Method(nameof(Math.Floor), typeof(double))] = "floor",
        [Method(nameof(Math.Floor), typeof(decimal))] = "floor",
        [Method(nameof(Math.Truncate), typeof(double))] = "trunc",
        [Method(nameof(Math.Truncate), typeof(decimal))] = "trunc",
    };

    private static readonly Dictionary<MethodInfo, string> DoubleFunctions = new()
    {
        [Method(nameof(Math.Sqrt), typeof(double))] = "sqrt",
        [Method(nameof(Math.Exp), typeof(double))] = "exp",
        [Method(nameof(Math.Log), typeof(double))] = "ln",
        [Method(nameof(Math.Log10), typeof(double))] = "log10",
        [Method(nameof(Math.Pow), typeof(double), typeof(double))] = "pow",
        [Method(nameof(Math.Sin), typeof(double))] = "sin",
        [Method(nameof(Math.Cos), typeof(double))] = "cos",
        [Method(nameof(Math.Tan), typeof(double))] = "tan",
        [Method(nameof(Math.Asin), typeof(double))] = "asin",
        [Method(nameof(Math.Acos), typeof(double))] = "acos",
        [Method(nameof(Math.Atan), typeof(double))] = "atan",
        [Method(nameof(Math.Atan2), typeof(double), typeof(double))] = "atan2",
        [typeof(double).GetMethod(nameof(double.DegreesToRadians), [typeof(double)])!] = "radians",
        [typeof(double).GetMethod(nameof(double.RadiansToDegrees), [typeof(double)])!] = "degrees",
    };

    private static readonly HashSet<MethodInfo> SignMethods =
    [
        Method(nameof(Math.Sign), typeof(double)),
        Method(nameof(Math.Sign), typeof(long)),
        Method(nameof(Math.Sign), typeof(int)),
        Method(nameof(Math.Sign), typeof(short)),
    ];

    private static readonly MethodInfo LogWithBase = Method(nameof(Math.Log), typeof(double), typeof(double));

    private static readonly MethodInfo Random =
        typeof(DbFunctionsExtensions).GetMethod(nameof(DbFunctionsExtensions.Random), [typeof(DbFunctions)])!;
    private static readonly HashSet<MethodInfo> RoundAwayFromZero =
    [
        Method(nameof(Math.Round), typeof(double), typeof(MidpointRounding)),
        Method(nameof(Math.Round), typeof(decimal), typeof(MidpointRounding)),
    ];

    private static readonly HashSet<MethodInfo> RoundDigitsAwayFromZero =
    [
        Method(nameof(Math.Round), typeof(double), typeof(int), typeof(MidpointRounding)),
        Method(nameof(Math.Round), typeof(decimal), typeof(int), typeof(MidpointRounding)),
    ];

    private readonly ISqlExpressionFactory _sql;
    private readonly IRelationalTypeMappingSource _typeMappingSource;

    public CamusMathTranslator(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
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
        if (instance is not null)
            return null;

        // EF Core's relational base writes RAND(), which the server does not have.
        if (method == Random)
            return _sql.Function("random", [], nullable: false, argumentsPropagateNullability: [],
                typeof(double), _typeMappingSource.FindMapping(typeof(double)));
        if (SameTypeFunctions.TryGetValue(method, out string? sameType))
            return SameType(sameType, arguments[0], method.ReturnType);

        if (DoubleFunctions.TryGetValue(method, out string? name))
            return Double(name, arguments);

        if (method == LogWithBase)
            return Double("log", [arguments[1], arguments[0]]);

        if (SignMethods.Contains(method))
        {
            SqlExpression argument = _sql.ApplyDefaultTypeMapping(arguments[0]);
            return _sql.Function("sign", [argument], nullable: true, argumentsPropagateNullability: [true],
                typeof(int), _typeMappingSource.FindMapping(typeof(int)));
        }

        if (RoundAwayFromZero.Contains(method) && IsAwayFromZero(arguments[1]))
            return SameType("round", arguments[0], method.ReturnType);

        if (RoundDigitsAwayFromZero.Contains(method) && IsAwayFromZero(arguments[2]))
        {
            SqlExpression value = _sql.ApplyDefaultTypeMapping(arguments[0]);
            SqlExpression digits = _sql.ApplyDefaultTypeMapping(arguments[1]);
            return _sql.Function("round", [value, digits], nullable: true, argumentsPropagateNullability: [true, true],
                method.ReturnType, value.TypeMapping);
        }

        return null;
    }

    private SqlExpression SameType(string name, SqlExpression argument, Type returnType)
    {
        argument = _sql.ApplyDefaultTypeMapping(argument);
        return _sql.Function(name, [argument], nullable: true, argumentsPropagateNullability: [true],
            returnType, argument.TypeMapping);
    }

    private SqlExpression Double(string name, IReadOnlyList<SqlExpression> arguments)
    {
        SqlExpression[] mapped = new SqlExpression[arguments.Count];
        bool[] propagates = new bool[arguments.Count];
        for (int i = 0; i < arguments.Count; i++)
        {
            mapped[i] = _sql.ApplyDefaultTypeMapping(arguments[i]);
            propagates[i] = true;
        }

        return _sql.Function(name, mapped, nullable: true, argumentsPropagateNullability: propagates,
            typeof(double), _typeMappingSource.FindMapping(typeof(double)));
    }

    private static bool IsAwayFromZero(SqlExpression mode)
        => mode is SqlConstantExpression { Value: MidpointRounding.AwayFromZero };

    private static MethodInfo Method(string name, params Type[] parameters)
        => typeof(Math).GetMethod(name, parameters)
           ?? throw new InvalidOperationException($"Math.{name} has no overload ({string.Join(", ", parameters.Select(p => p.Name))}).");
}
