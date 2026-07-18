
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using Google.Protobuf;
using Grpc = CamusDB.Grpc;

namespace CamusDB.Client.Transport;

/// <summary>
/// The single mapping point between the client's <see cref="ColumnValue"/> and the gRPC
/// <see cref="Grpc.Value"/> message — the gRPC-side analogue of the JSON codec in
/// <see cref="CamusResultSet"/>/<see cref="CamusCommand"/>. Covers all twelve
/// <see cref="ColumnType"/> cases per the protocol's compact-raw rules (Id in its own field, Date/DateTime
/// as raw UTC ticks, UUID as 16 big-endian bytes, Array carrying its element type).
///
/// <para>Unlike the positional REST rows — whose cells are decoded against a possibly mis-inferred
/// declared type — a gRPC <see cref="Grpc.Value"/> is self-describing, so <see cref="Decode"/> reads the
/// oneof case directly and the JOIN uuid mis-typing that bites the REST reader cannot occur here.</para>
/// </summary>
internal static class GrpcValueCodec
{
    /// <summary>Encodes a client <see cref="ColumnValue"/> (a bound parameter) into a gRPC <see cref="Grpc.Value"/>.</summary>
    internal static Grpc.Value Encode(in ColumnValue column)
    {
        switch (column.Type)
        {
            case ColumnType.Null:
                return new Grpc.Value { NullValue = Grpc.NullValue.Unset };

            case ColumnType.Id:
                return new Grpc.Value { IdValue = column.StrValue ?? "" };

            case ColumnType.Integer64:
                return new Grpc.Value { Int64Value = column.LongValue };

            case ColumnType.String:
                return new Grpc.Value { StringValue = column.StrValue ?? "" };

            case ColumnType.Bool:
                return new Grpc.Value { BoolValue = column.BoolValue };

            case ColumnType.Float64:
                return new Grpc.Value { Float64Value = column.FloatValue };

            case ColumnType.Float32:
                return new Grpc.Value { Float32Value = (float)column.FloatValue };

            case ColumnType.Bytes:
                return new Grpc.Value { BytesValue = ByteString.CopyFrom(column.BytesValue ?? []) };

            case ColumnType.Date:
                return new Grpc.Value { DateValue = column.LongValue };

            case ColumnType.DateTime:
                return new Grpc.Value { DatetimeValue = column.LongValue };

            case ColumnType.Uuid:
                return new Grpc.Value { UuidValue = ByteString.CopyFrom(UuidToBigEndianBytes(column)) };

            case ColumnType.Array:
                return EncodeArray(column);

            default:
                throw new CamusException("CADB0400", $"Cannot encode ColumnType {column.Type} for gRPC");
        }
    }

    /// <summary>Decodes a gRPC <see cref="Grpc.Value"/> (a result cell) into a client <see cref="ColumnValue"/>,
    /// reading the self-describing oneof case rather than trusting the declared column type.</summary>
    internal static ColumnValue Decode(Grpc.Value value)
    {
        switch (value.KindCase)
        {
            case Grpc.Value.KindOneofCase.None:
            case Grpc.Value.KindOneofCase.NullValue:
                return ColumnValue.Null;

            case Grpc.Value.KindOneofCase.IdValue:
                return new ColumnValue { Type = ColumnType.Id, StrValue = value.IdValue };

            case Grpc.Value.KindOneofCase.Int64Value:
                return new ColumnValue { Type = ColumnType.Integer64, LongValue = value.Int64Value };

            case Grpc.Value.KindOneofCase.StringValue:
                return new ColumnValue { Type = ColumnType.String, StrValue = value.StringValue };

            case Grpc.Value.KindOneofCase.BoolValue:
                return new ColumnValue { Type = ColumnType.Bool, BoolValue = value.BoolValue };

            case Grpc.Value.KindOneofCase.Float64Value:
                return new ColumnValue { Type = ColumnType.Float64, FloatValue = value.Float64Value };

            case Grpc.Value.KindOneofCase.Float32Value:
                return new ColumnValue { Type = ColumnType.Float32, FloatValue = value.Float32Value };

            case Grpc.Value.KindOneofCase.BytesValue:
                return new ColumnValue { Type = ColumnType.Bytes, BytesValue = value.BytesValue.ToByteArray() };

            case Grpc.Value.KindOneofCase.DateValue:
                return new ColumnValue { Type = ColumnType.Date, LongValue = value.DateValue };

            case Grpc.Value.KindOneofCase.DatetimeValue:
                return new ColumnValue { Type = ColumnType.DateTime, LongValue = value.DatetimeValue };

            case Grpc.Value.KindOneofCase.UuidValue:
                return DecodeUuid(value.UuidValue);

            case Grpc.Value.KindOneofCase.ArrayValue:
                return DecodeArray(value.ArrayValue);

            default:
                return ColumnValue.Null;
        }
    }

    private static Grpc.Value EncodeArray(in ColumnValue column)
    {
        Grpc.ArrayValue array = new() { ElementType = ToGrpcColumnType(column.ArrayElementType) };

        if (column.ArrayValues is { } items)
        {
            foreach (ColumnValue item in items)
                array.Items.Add(Encode(item));
        }

        return new Grpc.Value { ArrayValue = array };
    }

    private static ColumnValue DecodeArray(Grpc.ArrayValue array)
    {
        List<ColumnValue> values = new(array.Items.Count);
        foreach (Grpc.Value item in array.Items)
            values.Add(Decode(item));

        return new ColumnValue
        {
            Type = ColumnType.Array,
            ArrayValues = values,
            ArrayElementType = ToClientColumnType(array.ElementType),
        };
    }

    private static ColumnValue DecodeUuid(ByteString uuid)
    {
        if (uuid.Length != 16)
            throw new CamusException("CADB0000", $"UUID value must be 16 bytes, got {uuid.Length}");

        ReadOnlySpan<byte> bytes = uuid.Span;
        return new ColumnValue
        {
            Type = ColumnType.Uuid,
            UuidHigh = BinaryPrimitives.ReadInt64BigEndian(bytes[..8]),
            LongValue = BinaryPrimitives.ReadInt64BigEndian(bytes[8..]),
        };
    }

    // A UUID parameter reaches here as StrValue (canonical form, how CamusCommand builds it), or via the
    // raw high/low halves. Both collapse to the same 16 big-endian bytes the wire wants (high||low).
    private static byte[] UuidToBigEndianBytes(in ColumnValue column)
    {
        Guid guid;
        if (!string.IsNullOrEmpty(column.StrValue))
            guid = Guid.Parse(column.StrValue);
        else if (!string.IsNullOrEmpty(column.UuidValue))
            guid = Guid.Parse(column.UuidValue);
        else
            guid = column.AsGuid();

        return guid.ToByteArray(bigEndian: true);
    }

    // The gRPC and client ColumnType enums both mirror the engine's frozen integers exactly, so the
    // mapping is a straight integer cast in either direction.
    internal static Grpc.ColumnType ToGrpcColumnType(ColumnType type) => (Grpc.ColumnType)(int)type;

    internal static ColumnType ToClientColumnType(Grpc.ColumnType type) => (ColumnType)(int)type;
}
