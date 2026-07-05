
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

public sealed class CamusStringMethodTranslator : IMethodCallTranslator
{
    private static readonly MethodInfo StartsWithWithComparison = typeof(string)
        .GetMethod(nameof(string.StartsWith), [typeof(string), typeof(StringComparison)])!;

    private readonly ISqlExpressionFactory _sqlExpressionFactory;

    public CamusStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sqlExpressionFactory = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null || method != StartsWithWithComparison)
            return null;

        // Only translate constant prefix values; non-constant arguments require
        // runtime escaping (REPLACE chains) not yet confirmed supported by CamusDB.
        if (arguments[0] is not SqlConstantExpression { Value: string literalPrefix })
            return null;

        if (arguments[1] is not SqlConstantExpression { Value: StringComparison comparison })
            return null;

        bool ignoreCase = comparison is StringComparison.OrdinalIgnoreCase
            or StringComparison.CurrentCultureIgnoreCase
            or StringComparison.InvariantCultureIgnoreCase;

        // Escape LIKE special characters at translation time.
        string escaped = literalPrefix
            .Replace("\\", "\\\\", StringComparison.Ordinal)
            .Replace("%", "\\%", StringComparison.Ordinal)
            .Replace("_", "\\_", StringComparison.Ordinal);

        var escapeChar = _sqlExpressionFactory.Constant("\\");

        if (ignoreCase)
        {
            var lowerInstance = _sqlExpressionFactory.Function(
                "LOWER", [instance],
                nullable: true,
                argumentsPropagateNullability: [true],
                returnType: typeof(string));

            var lowerPattern = _sqlExpressionFactory.Constant(escaped.ToLowerInvariant() + "%");

            return _sqlExpressionFactory.Like(lowerInstance, lowerPattern, escapeChar);
        }

        var pattern = _sqlExpressionFactory.Constant(escaped + "%");
        return _sqlExpressionFactory.Like(instance, pattern, escapeChar);
    }
}
