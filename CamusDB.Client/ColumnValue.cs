
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// The wire representation of a single CamusDB column value. Mirrors the server's
/// <c>CamusDB.Core.CommandsExecutor.Models.ColumnValue</c>; backing storage per type:
/// <list type="bullet">
///   <item><see cref="ColumnType.Integer64"/> — <see cref="LongValue"/>.</item>
///   <item><see cref="ColumnType.Float64"/> / <see cref="ColumnType.Float32"/> — <see cref="FloatValue"/> (narrowed to float for Float32).</item>
///   <item><see cref="ColumnType.Bool"/> — <see cref="BoolValue"/>.</item>
///   <item><see cref="ColumnType.String"/> / <see cref="ColumnType.Id"/> — <see cref="StrValue"/>.</item>
///   <item><see cref="ColumnType.Date"/> / <see cref="ColumnType.DateTime"/> — <see cref="LongValue"/> as UTC <see cref="System.DateTime.Ticks"/> (Date truncated to midnight).</item>
///   <item><see cref="ColumnType.Bytes"/> — <see cref="BytesValue"/> (JSON base64).</item>
///   <item><see cref="ColumnType.Array"/> — <see cref="ArrayValues"/> + <see cref="ArrayElementType"/>.</item>
/// </list>
/// </summary>
public sealed class ColumnValue
{
    [JsonPropertyName("type")]
    public ColumnType Type { get; set; }

    [JsonPropertyName("strValue")]
    public string? StrValue { get; set; }

    [JsonPropertyName("longValue")]
    public long LongValue { get; set; }

    [JsonPropertyName("floatValue")]
    public double FloatValue { get; set; }

    [JsonPropertyName("boolValue")]
    public bool BoolValue { get; set; }

    /// <summary>Opaque byte payload for <see cref="ColumnType.Bytes"/>. Serialized as a base64 JSON string.</summary>
    [JsonPropertyName("bytesValue")]
    public byte[]? BytesValue { get; set; }

    /// <summary>Elements for <see cref="ColumnType.Array"/>. Each element is itself a <see cref="ColumnValue"/>.</summary>
    [JsonPropertyName("arrayValues")]
    public List<ColumnValue>? ArrayValues { get; set; }

    /// <summary>The declared scalar element type for <see cref="ColumnType.Array"/>.</summary>
    [JsonPropertyName("arrayElementType")]
    public ColumnType ArrayElementType { get; set; }

    /// <summary>
    /// ISO-8601 rendering the server includes in responses for <see cref="ColumnType.Date"/>
    /// (<c>yyyy-MM-dd</c>) and <see cref="ColumnType.DateTime"/> (round-trip <c>o</c>). Read-only; the
    /// canonical value is <see cref="LongValue"/>. Never sent back to the server.
    /// </summary>
    [JsonPropertyName("isoValue")]
    public string? IsoValue { get; set; }
}
