/**
 * This file is part of CamusDB
 *
 * Regression coverage for decoding `uuid` cells that the server mis-types as String. In JOIN
 * projections the server can report a `uuid` output column with declared type String (3) while still
 * sending the canonical `[high, low]` two-int64 wire form. The reader used to trust the declared type,
 * fail to recognize the array, and drop the value to Null — surfacing downstream as
 * "Column 'usersid' of type Null cannot be read as a Guid". The reader now recovers the Guid from the
 * unambiguous wire shape. These tests run offline against hand-built wire payloads (no server needed).
 */

using System.Text.Json;

namespace CamusDB.Client.Tests;

public class TestUuidDecoding
{
    // A known UUID and the [high, low] big-endian halves the server sends for it (captured from a live
    // JOIN response for usersid 019f49cc-0fd0-734b-a6d9-32562059eecd).
    private const long High = 116893256122397515L;
    private const long Low = -6424048047975960883L;
    private static readonly Guid Expected = Guid.Parse("019f49cc-0fd0-734b-a6d9-32562059eecd");

    private static CamusDataReader ReaderFor(ColumnType declaredType)
    {
        var columns = new List<CamusColumnSchema>
        {
            new() { Name = "usersid", Type = declaredType },
        };

        using JsonDocument doc = JsonDocument.Parse($"[[[{High}, {Low}]]]");
        CamusResultSet rs = CamusResultSet.FromWire(columns, doc.RootElement.Clone());
        return new CamusDataReader(rs);
    }

    [Fact]
    public void UuidWireFormDecodesEvenWhenColumnMistypedAsString()
    {
        // The exact Vlitz failure: declared String (3), payload [high, low].
        using CamusDataReader reader = ReaderFor(ColumnType.String);
        Assert.True(reader.Read());

        Assert.Equal(ColumnType.Uuid, reader.GetColumnValue(0).Type);
        Assert.Equal(Expected, reader.GetGuid(0));
        Assert.Equal(Expected, reader.GetFieldValue<Guid>(0));
    }

    [Fact]
    public void UuidWireFormStillDecodesWhenColumnTypedCorrectly()
    {
        using CamusDataReader reader = ReaderFor(ColumnType.Uuid);
        Assert.True(reader.Read());

        Assert.Equal(ColumnType.Uuid, reader.GetColumnValue(0).Type);
        Assert.Equal(Expected, reader.GetGuid(0));
    }
}
