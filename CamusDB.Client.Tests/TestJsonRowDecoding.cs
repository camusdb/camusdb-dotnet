/**
 * This file is part of CamusDB
 *
 * Offline coverage for the two decoders of a positional JSON row: the DOM decoder of the buffered
 * endpoint (CamusResultSet.FromWire) and the Utf8JsonReader decoder of the streaming endpoint
 * (CamusResultSet.TryDecodeRowInto). The wire row encoding is the same for both, so every row here must
 * decode to the same cells, or fail with the same exception type. No server is required.
 */

using System.Text;
using System.Text.Json;

namespace CamusDB.Client.Tests;

public class TestJsonRowDecoding
{
    private static readonly ColumnType[] Mixed =
        [ColumnType.Id, ColumnType.Integer64, ColumnType.Bool, ColumnType.Float64, ColumnType.String];

    public static TheoryData<ColumnType[], string> Rows() => new()
    {
        { Mixed, """["a", 5, true, 1.5, "x"]""" },
        { Mixed, """[null, null, null, null, null]""" },
        { Mixed, """["a"]""" },
        { Mixed, """["a", 1, false, 2.0, "x", "extra", [1, 2], {"k": [3]}]""" },
        { Mixed, """["a\"bé", -9223372036854775808, false, 3, "😀"]""" },
        { Mixed, """[]""" },
        { [ColumnType.Float64, ColumnType.Float32], """[3, 2.5]""" },
        { [ColumnType.String, ColumnType.String, ColumnType.String], """[7, 1.5, 1e300]""" },
        { [ColumnType.String], """[18446744073709551616]""" },
        { [ColumnType.Bytes, ColumnType.Date, ColumnType.DateTime], """["AQID", 638000000000000000, 0]""" },
        { [ColumnType.Uuid], """[[1, -2]]""" },
        { [ColumnType.Uuid], """["0f8fad5b-d9cb-469f-a165-70867728950e"]""" },
        { [ColumnType.Uuid], """[[1, 2, 3]]""" },
        { [ColumnType.Uuid], """[[]]""" },
        { [ColumnType.Uuid], """[[["x"], 1, 2]]""" },
        { [ColumnType.Uuid], """[5]""" },
        { [ColumnType.Uuid], """[true]""" },
        { [ColumnType.Uuid], """[{"a": 1}]""" },
        { [ColumnType.String], """[[4, 5]]""" },
        { [ColumnType.String], """[[1, 2, 3]]""" },
        { [ColumnType.String], """[{"k": [1]}]""" },
        { [ColumnType.Integer64], """[[{"a": 1}]]""" },
        { [ColumnType.Array], """[[1, 2.5, "s", true, null, [1], {"a": 1}]]""" },
        { [ColumnType.Array], """[[]]""" },
        { [ColumnType.Array], """[5]""" },
        { [ColumnType.Array], """[{"a": [1]}]""" },
        { [ColumnType.Array, ColumnType.Integer64], """[[1, 2], 3]""" },
    };

    [Theory]
    [MemberData(nameof(Rows))]
    public void StreamingDecoderAgreesWithTheBufferedDecoder(ColumnType[] types, string row)
    {
        ColumnValue[] expected = DecodeWithDom(types, row);

        // Filled with a value first, so a cell the decoder fails to write shows up.
        ColumnValue[] actual = Enumerable.Repeat(new ColumnValue { Type = ColumnType.Integer64, LongValue = 99 }, types.Length).ToArray();

        Assert.True(CamusResultSet.TryDecodeRowInto(Encoding.UTF8.GetBytes(row), types, actual));

        for (int i = 0; i < types.Length; i++)
            AssertSameCell(expected[i], actual[i]);
    }

    public static TheoryData<ColumnType[], string> BadRows() => new()
    {
        { [ColumnType.Uuid], """[["x", 1]]""" },
        { [ColumnType.Uuid], """[[1, "x"]]""" },
        { [ColumnType.Uuid], """[[1e30, 1]]""" },
        { [ColumnType.Uuid], """[[1.5, 1]]""" },
        { [ColumnType.String], """[[1, {"a": 1}]]""" },
        { [ColumnType.Id], """[5]""" },
        { [ColumnType.Date], """["x"]""" },
        { [ColumnType.DateTime], """[1.5]""" },
        { [ColumnType.Bytes], """["!!"]""" },
        { [ColumnType.Bytes], """[5]""" },
        { Mixed, """["a", 1, tru""" },
        { Mixed, """["a", 1""" },
        { Mixed, """["a"] x""" },
        { Mixed, """["a"] ["b"]""" },
        { Mixed, """["a",]""" },
    };

    [Theory]
    [MemberData(nameof(BadRows))]
    public void StreamingDecoderFailsAsTheBufferedDecoderFails(ColumnType[] types, string row)
    {
        Exception expected = Assert.ThrowsAny<Exception>(() => DecodeWithDom(types, row));
        Exception actual = Assert.ThrowsAny<Exception>(
            () => CamusResultSet.TryDecodeRowInto(Encoding.UTF8.GetBytes(row), types, new ColumnValue[types.Length]));

        Assert.Equal(Category(expected), Category(actual));
    }

    [Theory]
    [InlineData("""{"status":"ok","total":2}""")]
    [InlineData("""5""")]
    public void ALineThatIsNotAnArrayIsNotARow(string line)
    {
        ColumnValue[] cells = [new ColumnValue { Type = ColumnType.Integer64, LongValue = 7 }];

        Assert.False(CamusResultSet.TryDecodeRowInto(Encoding.UTF8.GetBytes(line), [ColumnType.Integer64], cells));
        Assert.Equal(7, cells[0].LongValue);
    }

    // The DOM decoder, as the buffered endpoint runs it: the row is parsed as one document, and a
    // malformed row fails the parse. The streaming source parses one line at a time, so a row is parsed
    // alone here too; the rows array around it is built from the decoded row element.
    private static ColumnValue[] DecodeWithDom(ColumnType[] types, string row)
    {
        using JsonDocument line = JsonDocument.Parse(row);

        string columns = "[" + string.Join(",", types.Select((t, i) => $$"""{"name":"c{{i}}","type":{{(int)t}}}""")) + "]";
        using JsonDocument columnsDoc = JsonDocument.Parse(columns);
        using JsonDocument rowsDoc = JsonDocument.Parse("[" + line.RootElement.GetRawText() + "]");

        CamusResultSet result = CamusResultSet.FromWire(columnsDoc.RootElement, rowsDoc.RootElement);

        return Enumerable.Range(0, types.Length).Select(c => result.GetCell(0, c)).ToArray();
    }

    // JsonDocument and Utf8JsonReader throw the same internal JsonException subtype, so the category is
    // the public type a caller can catch.
    private static Type Category(Exception ex) => ex switch
    {
        JsonException => typeof(JsonException),
        FormatException => typeof(FormatException),
        InvalidOperationException => typeof(InvalidOperationException),
        _ => ex.GetType()
    };

    private static void AssertSameCell(ColumnValue expected, ColumnValue actual)
    {
        Assert.Equal(expected.Type, actual.Type);
        Assert.Equal(expected.LongValue, actual.LongValue);
        Assert.Equal(expected.UuidHigh, actual.UuidHigh);
        Assert.Equal(expected.FloatValue, actual.FloatValue);
        Assert.Equal(expected.BoolValue, actual.BoolValue);
        Assert.Equal(expected.StrValue, actual.StrValue);
        Assert.Equal(expected.UuidValue, actual.UuidValue);
        Assert.Equal(expected.BytesValue, actual.BytesValue);
        Assert.Equal(expected.ArrayElementType, actual.ArrayElementType);
        Assert.Equal(expected.ArrayValues is null, actual.ArrayValues is null);

        if (expected.ArrayValues is not null)
        {
            Assert.Equal(expected.ArrayValues.Count, actual.ArrayValues!.Count);

            for (int i = 0; i < expected.ArrayValues.Count; i++)
                AssertSameCell(expected.ArrayValues[i], actual.ArrayValues[i]);
        }
    }
}
