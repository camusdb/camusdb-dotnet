/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data.Common;
using System.Text.Json;
using System.Text.Json.Nodes;
using CamusDB.Client;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Maps a property to CamusDB's <c>JSONB</c> column, which holds one JSON document. A
/// <see cref="JsonDocument"/>, <see cref="JsonElement"/>, <see cref="JsonNode"/>, <see cref="JsonObject"/>
/// or <see cref="JsonArray"/> property maps to it by default. A <see cref="string"/> property maps to it
/// with <c>HasColumnType("jsonb")</c> (or <c>"json"</c>, an alias), and then holds the document text.
///
/// <para>The parameter is stamped <see cref="ColumnType.Jsonb"/>, so the server parses the text as a
/// document, also where the SQL compares it with a document. A literal is the typed
/// <c>JSONB '…'</c>.</para>
///
/// <para>The reader gives the document in the PostgreSQL text form. A value that the application writes
/// therefore reads back with canonical spacing and key order, not with its own text.</para>
/// </summary>
public sealed class CamusJsonbTypeMapping : RelationalTypeMapping
{
    private CamusJsonbTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters) { }

    /// <summary>The mapping of <paramref name="clrType"/> to a <c>JSONB</c> column.</summary>
    public static CamusJsonbTypeMapping Create(Type clrType)
    {
        ValueComparer? comparer = ComparerFor(clrType);

        return new(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(clrType, comparer: comparer, keyComparer: comparer),
            "jsonb",
            StoreTypePostfix.None,
            System.Data.DbType.String));
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new CamusJsonbTypeMapping(parameters);

    protected override void ConfigureParameter(DbParameter parameter)
    {
        base.ConfigureParameter(parameter);

        if (parameter is CamusParameter camusParameter)
            camusParameter.ColumnType = ColumnType.Jsonb;
    }

    protected override string GenerateNonNullSqlLiteral(object value)
        => "JSONB " + CamusSqlSyntax.Literal(CamusJsonb.FromParameter("literal", value), "a JSONB literal");

    // A JsonDocument and a JsonElement cannot change, so a snapshot is the same value, and two values are
    // equal when their text is. A JsonNode can change in place, so its snapshot is a deep copy, and change
    // tracking sees an edit of the tree. A string uses the default comparer.
    private static ValueComparer? ComparerFor(Type clrType)
    {
        if (clrType == typeof(JsonDocument))
            return new ValueComparer<JsonDocument>(
                (a, b) => ReferenceEquals(a, b) || (a != null && b != null && a.RootElement.GetRawText() == b.RootElement.GetRawText()),
                d => d.RootElement.GetRawText().GetHashCode(),
                d => d);

        if (clrType == typeof(JsonElement))
            return new ValueComparer<JsonElement>(
                (a, b) => a.GetRawText() == b.GetRawText(),
                e => e.GetRawText().GetHashCode(),
                e => e);

        if (clrType == typeof(JsonNode))
            return NodeComparer<JsonNode>();

        if (clrType == typeof(JsonObject))
            return NodeComparer<JsonObject>();

        if (clrType == typeof(JsonArray))
            return NodeComparer<JsonArray>();

        return null;
    }

    private static ValueComparer<T> NodeComparer<T>() where T : JsonNode => new(
        (a, b) => JsonNode.DeepEquals(a, b),
        n => n == null ? 0 : n.ToJsonString().GetHashCode(),
        n => n == null ? null! : (T)n.DeepClone());
}
