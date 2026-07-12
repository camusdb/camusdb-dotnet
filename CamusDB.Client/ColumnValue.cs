
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
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
///   <item><see cref="ColumnType.Uuid"/> — the 128-bit value split into two big-endian 64-bit halves:
///     the high bits in <see cref="UuidHigh"/> and the low bits in <see cref="LongValue"/>. The server
///     also emits the canonical string form in <see cref="UuidValue"/> for readability. Use
///     <see cref="AsGuid"/> to reconstruct the <see cref="System.Guid"/>.</item>
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

    /// <summary>
    /// High 64 bits (big-endian RFC 4122 bytes 0..7) of a <see cref="ColumnType.Uuid"/> value; the low
    /// 64 bits live in <see cref="LongValue"/>. Zero for all other types.
    /// </summary>
    [JsonPropertyName("uuidHigh")]
    public long UuidHigh { get; set; }

    /// <summary>
    /// Canonical lowercase hyphenated string form of a <see cref="ColumnType.Uuid"/> value the server
    /// includes in responses for readability (e.g. <c>"550e8400-e29b-41d4-a716-446655440000"</c>). The
    /// authoritative value is the <see cref="UuidHigh"/>/<see cref="LongValue"/> pair; the server ignores
    /// this field on input (a UUID parameter is sent via <see cref="StrValue"/>).
    /// </summary>
    [JsonPropertyName("uuidValue")]
    public string? UuidValue { get; set; }

    /// <summary>
    /// Reconstructs the <see cref="System.Guid"/> for a <see cref="ColumnType.Uuid"/> value. Prefers the
    /// raw big-endian halves and falls back to parsing <see cref="UuidValue"/> when they are absent.
    /// </summary>
    public Guid AsGuid()
    {
        if (UuidHigh == 0 && LongValue == 0 && !string.IsNullOrEmpty(UuidValue))
            return Guid.Parse(UuidValue);

        Span<byte> bytes = stackalloc byte[16];
        BinaryPrimitives.WriteUInt64BigEndian(bytes[..8], (ulong)UuidHigh);
        BinaryPrimitives.WriteUInt64BigEndian(bytes[8..], (ulong)LongValue);
        return new Guid(bytes, bigEndian: true);
    }
}
