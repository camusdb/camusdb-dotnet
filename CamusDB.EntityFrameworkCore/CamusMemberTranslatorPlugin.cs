
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

public sealed class CamusMemberTranslatorPlugin : IMemberTranslatorPlugin
{
    public CamusMemberTranslatorPlugin(ISqlExpressionFactory sqlExpressionFactory)
    {
        Translators = [new CamusStringMemberTranslator(sqlExpressionFactory)];
    }

    public IEnumerable<IMemberTranslator> Translators { get; }
}

/// <summary>Translates <see cref="string.Length"/> to CamusDB's <c>length</c> scalar function.</summary>
public sealed class CamusStringMemberTranslator : IMemberTranslator
{
    private readonly ISqlExpressionFactory _sql;

    public CamusStringMemberTranslator(ISqlExpressionFactory sqlExpressionFactory)
    {
        _sql = sqlExpressionFactory;
    }

    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null || member.DeclaringType != typeof(string) || member.Name != nameof(string.Length))
            return null;

        return _sql.Function(
            "length",
            [instance],
            nullable: true,
            argumentsPropagateNullability: [true],
            returnType: typeof(int));
    }
}
