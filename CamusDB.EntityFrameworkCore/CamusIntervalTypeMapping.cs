/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Data.Common;
using CamusDB.Client;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Maps <see cref="TimeSpan"/> to the SQL <c>interval</c> type. An interval is a value of an
/// expression only: no table column can have this type, and <see cref="CamusModelValidator"/> refuses a
/// <see cref="TimeSpan"/> property on a table. The mapping is for query expressions, such as
/// <c>e.Finished - e.Started</c>, a <see cref="TimeSpan"/> parameter, and a projection of either.
///
/// <list type="bullet">
/// <item>A literal is <c>INTERVAL 'P1DT2H'</c>. A literal can also hold a <see cref="CamusInterval"/>, which
/// the translators use for a month, because a <see cref="TimeSpan"/> has no months.</item>
/// <item>A parameter binds as its ISO-8601 text, and the SQL generator writes
/// <c>CAST(@p AS interval)</c>, because the server does not read a string as an interval by itself.</item>
/// <item>A result arrives as ISO-8601 text in a string column. The reader parses it in
/// <c>GetFieldValue&lt;TimeSpan&gt;</c>.</item>
/// </list>
/// </summary>
public sealed class CamusIntervalTypeMapping : TimeSpanTypeMapping
{
    public const string StoreTypeName = "interval";

    public static CamusIntervalTypeMapping Instance { get; } = new();

    public CamusIntervalTypeMapping() : base(StoreTypeName, System.Data.DbType.String) { }

    private CamusIntervalTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters) { }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new CamusIntervalTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => $"INTERVAL '{CamusIntervalText(value)}'";

    protected override void ConfigureParameter(DbParameter parameter)
    {
        base.ConfigureParameter(parameter);

        if (parameter is CamusParameter camusParameter)
            camusParameter.ColumnType = ColumnType.String;
    }

    private static string CamusIntervalText(object value) => value switch
    {
        CamusInterval interval => interval.ToString(),
        TimeSpan span => CamusInterval.FromTimeSpan(span).ToString(),
        _ => throw new InvalidCastException($"An interval literal cannot hold a {value.GetType().Name}."),
    };
}
