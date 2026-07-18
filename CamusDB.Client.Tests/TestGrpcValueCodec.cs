/**
 * This file is part of CamusDB
 *
 * Offline round-trip coverage for the gRPC value codec (CamusDB.Client.Transport.GrpcValueCodec): a
 * client ColumnValue encoded into a gRPC Value and decoded back must survive unchanged for all twelve
 * ColumnType cases. No server is required — the codec is a pure function.
 */

using System.Buffers.Binary;
using CamusDB.Client.Transport;
using Grpc = CamusDB.Grpc;

namespace CamusDB.Client.Tests;

public class TestGrpcValueCodec
{
    [Fact]
    public void NullRoundTrips()
    {
        Grpc.Value encoded = GrpcValueCodec.Encode(new ColumnValue { Type = ColumnType.Null });
        Assert.Equal(Grpc.Value.KindOneofCase.NullValue, encoded.KindCase);
        Assert.Equal(ColumnType.Null, GrpcValueCodec.Decode(encoded).Type);
    }

    [Fact]
    public void IdRoundTrips()
    {
        ColumnValue decoded = RoundTrip(new ColumnValue { Type = ColumnType.Id, StrValue = "68000000000000000000abcd" });
        Assert.Equal(ColumnType.Id, decoded.Type);
        Assert.Equal("68000000000000000000abcd", decoded.StrValue);
    }

    [Fact]
    public void Integer64RoundTrips()
    {
        ColumnValue decoded = RoundTrip(new ColumnValue { Type = ColumnType.Integer64, LongValue = -9223372036854775808L });
        Assert.Equal(ColumnType.Integer64, decoded.Type);
        Assert.Equal(-9223372036854775808L, decoded.LongValue);
    }

    [Fact]
    public void StringRoundTrips()
    {
        ColumnValue decoded = RoundTrip(new ColumnValue { Type = ColumnType.String, StrValue = "hello — world" });
        Assert.Equal(ColumnType.String, decoded.Type);
        Assert.Equal("hello — world", decoded.StrValue);
    }

    [Fact]
    public void BoolRoundTrips()
    {
        Assert.True(RoundTrip(new ColumnValue { Type = ColumnType.Bool, BoolValue = true }).BoolValue);
        Assert.False(RoundTrip(new ColumnValue { Type = ColumnType.Bool, BoolValue = false }).BoolValue);
    }

    [Fact]
    public void Float64RoundTrips()
    {
        ColumnValue decoded = RoundTrip(new ColumnValue { Type = ColumnType.Float64, FloatValue = 3.141592653589793 });
        Assert.Equal(ColumnType.Float64, decoded.Type);
        Assert.Equal(3.141592653589793, decoded.FloatValue, 15);
    }

    [Fact]
    public void Float32RoundTrips()
    {
        ColumnValue decoded = RoundTrip(new ColumnValue { Type = ColumnType.Float32, FloatValue = 1.5f });
        Assert.Equal(ColumnType.Float32, decoded.Type);
        Assert.Equal(1.5, decoded.FloatValue, 6);
    }

    [Fact]
    public void BytesRoundTrips()
    {
        byte[] payload = [0, 1, 2, 250, 255];
        ColumnValue decoded = RoundTrip(new ColumnValue { Type = ColumnType.Bytes, BytesValue = payload });
        Assert.Equal(ColumnType.Bytes, decoded.Type);
        Assert.Equal(payload, decoded.BytesValue);
    }

    [Fact]
    public void DateRoundTrips()
    {
        long ticks = new DateTime(2026, 7, 18, 0, 0, 0, DateTimeKind.Utc).Ticks;
        ColumnValue decoded = RoundTrip(new ColumnValue { Type = ColumnType.Date, LongValue = ticks });
        Assert.Equal(ColumnType.Date, decoded.Type);
        Assert.Equal(ticks, decoded.LongValue);
    }

    [Fact]
    public void DateTimeRoundTrips()
    {
        long ticks = new DateTime(2026, 7, 18, 13, 45, 12, DateTimeKind.Utc).Ticks;
        ColumnValue decoded = RoundTrip(new ColumnValue { Type = ColumnType.DateTime, LongValue = ticks });
        Assert.Equal(ColumnType.DateTime, decoded.Type);
        Assert.Equal(ticks, decoded.LongValue);
    }

    [Fact]
    public void UuidFromHalvesRoundTrips()
    {
        // The [high, low] halves for 019f49cc-0fd0-734b-a6d9-32562059eecd, as the server sends them.
        ColumnValue source = new()
        {
            Type = ColumnType.Uuid,
            UuidHigh = 116893256122397515L,
            LongValue = -6424048047975960883L,
        };

        Grpc.Value encoded = GrpcValueCodec.Encode(source);
        Assert.Equal(Grpc.Value.KindOneofCase.UuidValue, encoded.KindCase);
        Assert.Equal(16, encoded.UuidValue.Length);

        ColumnValue decoded = GrpcValueCodec.Decode(encoded);
        Assert.Equal(ColumnType.Uuid, decoded.Type);
        Assert.Equal(Guid.Parse("019f49cc-0fd0-734b-a6d9-32562059eecd"), decoded.AsGuid());
    }

    [Fact]
    public void UuidFromCanonicalStringEncodesToBigEndianBytes()
    {
        // A UUID *parameter* reaches the codec as StrValue (how CamusCommand builds it). It must encode to
        // the same 16 big-endian bytes (high||low) the wire expects.
        Guid guid = Guid.Parse("019f49cc-0fd0-734b-a6d9-32562059eecd");
        ColumnValue param = new() { Type = ColumnType.Uuid, StrValue = guid.ToString() };

        Grpc.Value encoded = GrpcValueCodec.Encode(param);
        Assert.Equal(16, encoded.UuidValue.Length);
        Assert.Equal(guid.ToByteArray(bigEndian: true), encoded.UuidValue.ToByteArray());

        Assert.Equal(guid, GrpcValueCodec.Decode(encoded).AsGuid());
    }

    [Fact]
    public void ArrayRoundTripsWithElementType()
    {
        ColumnValue source = new()
        {
            Type = ColumnType.Array,
            ArrayElementType = ColumnType.Integer64,
            ArrayValues =
            [
                new ColumnValue { Type = ColumnType.Integer64, LongValue = 1 },
                new ColumnValue { Type = ColumnType.Integer64, LongValue = 2 },
                new ColumnValue { Type = ColumnType.Integer64, LongValue = 3 },
            ],
        };

        ColumnValue decoded = RoundTrip(source);

        Assert.Equal(ColumnType.Array, decoded.Type);
        Assert.Equal(ColumnType.Integer64, decoded.ArrayElementType);
        Assert.NotNull(decoded.ArrayValues);
        Assert.Equal(3, decoded.ArrayValues!.Count);
        Assert.Equal(new long[] { 1, 2, 3 }, decoded.ArrayValues.Select(v => v.LongValue));
    }

    [Fact]
    public void EmptyArrayPreservesElementType()
    {
        ColumnValue decoded = RoundTrip(new ColumnValue
        {
            Type = ColumnType.Array,
            ArrayElementType = ColumnType.String,
            ArrayValues = [],
        });

        Assert.Equal(ColumnType.Array, decoded.Type);
        Assert.Equal(ColumnType.String, decoded.ArrayElementType);
        Assert.Empty(decoded.ArrayValues!);
    }

    [Fact]
    public void UuidWireBytesAreBigEndianHighThenLow()
    {
        // Guard the exact wire layout: first 8 bytes are the high half, last 8 the low half, big-endian.
        ColumnValue source = new() { Type = ColumnType.Uuid, UuidHigh = 0x0102030405060708L, LongValue = 0x1112131415161718L };

        byte[] bytes = GrpcValueCodec.Encode(source).UuidValue.ToByteArray();

        Assert.Equal(0x0102030405060708L, BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan(0, 8)));
        Assert.Equal(0x1112131415161718L, BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan(8, 8)));
    }

    private static ColumnValue RoundTrip(in ColumnValue value) => GrpcValueCodec.Decode(GrpcValueCodec.Encode(value));
}
