/**
 * This file is part of CamusDB
 *
 * Offline coverage for CamusDataReader.GetChars (chunked reads of a text column) and for the typed
 * array materializers behind GetFieldValue<T>. Both were rewritten to stop building an intermediate
 * array per call, so these tests pin the return counts, the copied contents and the exception types the
 * previous implementation produced.
 */

namespace CamusDB.Client.Tests;

public class TestDataReaderChunking
{
    private static CamusDataReader ReaderFor(string text)
    {
        CamusResultSet result = CamusResultSet.FromRows(
        [
            new Dictionary<string, ColumnValue>
            {
                ["s"] = new() { Type = ColumnType.String, StrValue = text },
            },
        ]);

        CamusDataReader reader = new(result);
        Assert.True(reader.Read());
        return reader;
    }

    private static CamusDataReader ReaderForArray(ColumnType elementType, params ColumnValue[] elements)
    {
        CamusResultSet result = CamusResultSet.FromRows(
        [
            new Dictionary<string, ColumnValue>
            {
                ["a"] = new()
                {
                    Type = ColumnType.Array,
                    ArrayElementType = elementType,
                    ArrayValues = [.. elements],
                },
            },
        ]);

        CamusDataReader reader = new(result);
        Assert.True(reader.Read());
        return reader;
    }

    private static ColumnValue Int64(long value) => new() { Type = ColumnType.Integer64, LongValue = value };

    private static ColumnValue Text(string value) => new() { Type = ColumnType.String, StrValue = value };

    // ─── GetChars ─────────────────────────────────────────────────────────────

    [Fact]
    public void ChunkedReadsReassembleTheWholeValue()
    {
        const string text = "the quick brown fox jumps over the lazy dog";
        using CamusDataReader reader = ReaderFor(text);

        char[] buffer = new char[8];
        System.Text.StringBuilder rebuilt = new();
        long offset = 0;

        while (true)
        {
            long copied = reader.GetChars(0, offset, buffer, 0, buffer.Length);
            if (copied == 0)
                break;

            rebuilt.Append(buffer, 0, (int)copied);
            offset += copied;
        }

        Assert.Equal(text, rebuilt.ToString());
    }

    [Fact]
    public void NullBufferReportsTheAvailableCount()
    {
        using CamusDataReader reader = ReaderFor("abcdef");

        Assert.Equal(6, reader.GetChars(0, 0, null, 0, 6));
        Assert.Equal(4, reader.GetChars(0, 2, null, 0, 100));
        Assert.Equal(2, reader.GetChars(0, 0, null, 0, 2));
    }

    [Fact]
    public void OffsetAtOrPastTheEndCopiesNothing()
    {
        using CamusDataReader reader = ReaderFor("abc");

        Assert.Equal(0, reader.GetChars(0, 3, new char[4], 0, 4));
        Assert.Equal(0, reader.GetChars(0, 99, new char[4], 0, 4));
        Assert.Equal(0, reader.GetChars(0, 3, null, 0, 4));
    }

    [Fact]
    public void EmptyValueCopiesNothing()
    {
        using CamusDataReader reader = ReaderFor("");

        Assert.Equal(0, reader.GetChars(0, 0, new char[4], 0, 4));
        Assert.Equal(0, reader.GetChars(0, 0, null, 0, 4));
    }

    [Fact]
    public void PartialChunkCopiesOnlyTheRequestedRange()
    {
        using CamusDataReader reader = ReaderFor("abcdef");

        char[] buffer = new char[10];
        Array.Fill(buffer, '.');

        Assert.Equal(3, reader.GetChars(0, 1, buffer, 2, 3));
        Assert.Equal("..bcd.....", new string(buffer));
    }

    [Fact]
    public void RequestBeyondTheEndIsClampedToWhatIsLeft()
    {
        using CamusDataReader reader = ReaderFor("abcdef");

        char[] buffer = new char[10];
        Assert.Equal(2, reader.GetChars(0, 4, buffer, 0, 10));
        Assert.Equal("ef", new string(buffer, 0, 2));
    }

    [Fact]
    public void SurrogatePairSplitByAChunkBoundaryKeepsCodeUnitOffsets()
    {
        // Offsets are UTF-16 code units, so a chunk may divide a surrogate pair — the halves must still
        // rejoin into the original string.
        const string text = "a\U0001F600b";
        using CamusDataReader reader = ReaderFor(text);

        char[] buffer = new char[2];
        System.Text.StringBuilder rebuilt = new();

        for (long offset = 0; ; offset += 2)
        {
            long copied = reader.GetChars(0, offset, buffer, 0, 2);
            if (copied == 0)
                break;

            rebuilt.Append(buffer, 0, (int)copied);
        }

        Assert.Equal(text, rebuilt.ToString());
        Assert.Equal(4, text.Length);
    }

    [Fact]
    public void NegativeOffsetThrows()
    {
        using CamusDataReader reader = ReaderFor("abc");

        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetChars(0, -1, new char[4], 0, 4));
        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetChars(0, -1, null, 0, 4));
    }

    [Fact]
    public void NegativeLengthThrows()
    {
        using CamusDataReader reader = ReaderFor("abc");

        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetChars(0, 0, new char[4], 0, -1));
    }

    [Fact]
    public void InvalidDestinationRangeThrows()
    {
        using CamusDataReader reader = ReaderFor("abcdef");

        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetChars(0, 0, new char[4], -1, 2));
        Assert.Throws<ArgumentOutOfRangeException>(() => reader.GetChars(0, 0, new char[4], 9, 2));
        Assert.Throws<ArgumentException>(() => reader.GetChars(0, 0, new char[4], 3, 6));
    }

    // ─── Typed arrays ─────────────────────────────────────────────────────────

    [Fact]
    public void ObjectArrayIsReturnedDirectly()
    {
        using CamusDataReader reader = ReaderForArray(ColumnType.Integer64, Int64(1), Int64(2));

        object?[] values = reader.GetFieldValue<object?[]>(0);

        Assert.Equal(new object?[] { 1L, 2L }, values);
    }

    [Fact]
    public void TypedArraysMaterializeWithTheirValues()
    {
        using CamusDataReader longs = ReaderForArray(ColumnType.Integer64, Int64(1), Int64(-2), Int64(3));
        Assert.Equal(new[] { 1L, -2L, 3L }, longs.GetFieldValue<long[]>(0));

        using CamusDataReader doubles = ReaderForArray(
            ColumnType.Float64,
            new ColumnValue { Type = ColumnType.Float64, FloatValue = 1.5 },
            new ColumnValue { Type = ColumnType.Float64, FloatValue = -0.25 });
        Assert.Equal(new[] { 1.5, -0.25 }, doubles.GetFieldValue<double[]>(0));

        using CamusDataReader bools = ReaderForArray(
            ColumnType.Bool,
            new ColumnValue { Type = ColumnType.Bool, BoolValue = true },
            new ColumnValue { Type = ColumnType.Bool, BoolValue = false });
        Assert.Equal(new[] { true, false }, bools.GetFieldValue<bool[]>(0));

        using CamusDataReader strings = ReaderForArray(ColumnType.String, Text("a"), Text("b"));
        Assert.Equal(new[] { "a", "b" }, strings.GetFieldValue<string[]>(0));

        Guid guid = Guid.NewGuid();
        using CamusDataReader guids = ReaderForArray(
            ColumnType.Uuid,
            new ColumnValue { Type = ColumnType.Uuid, UuidValue = guid.ToString() });
        Assert.Equal(new[] { guid }, guids.GetFieldValue<Guid[]>(0));
    }

    [Fact]
    public void EmptyTypedArrayMaterializesEmpty()
    {
        using CamusDataReader reader = ReaderForArray(ColumnType.Integer64);

        Assert.Empty(reader.GetFieldValue<long[]>(0));
        Assert.Empty(reader.GetFieldValue<object?[]>(0));
    }

    [Fact]
    public void NullElementsKeepTheirGeneralBehaviour()
    {
        using CamusDataReader reader = ReaderForArray(ColumnType.String, Text("a"), ColumnValue.Null, Text("c"));

        Assert.Equal(new string?[] { "a", null, "c" }, reader.GetFieldValue<string[]>(0));
        Assert.Equal(new object?[] { "a", null, "c" }, reader.GetFieldValue<object?[]>(0));
    }

    [Fact]
    public void MismatchedElementTypesStillConvert()
    {
        // Integer cells requested as double[] take the general conversion path, which converts rather
        // than failing.
        using CamusDataReader reader = ReaderForArray(ColumnType.Integer64, Int64(1), Int64(2));

        Assert.Equal(new[] { 1.0, 2.0 }, reader.GetFieldValue<double[]>(0));
    }

    [Fact]
    public void OverflowOnConversionStillThrows()
    {
        using CamusDataReader reader = ReaderForArray(ColumnType.Integer64, Int64(long.MaxValue));

        Assert.Throws<OverflowException>(() => reader.GetFieldValue<int[]>(0));
    }

    [Fact]
    public void RepeatedCallsReturnIndependentArrays()
    {
        using CamusDataReader reader = ReaderForArray(ColumnType.Integer64, Int64(1), Int64(2));

        long[] first = reader.GetFieldValue<long[]>(0);
        long[] second = reader.GetFieldValue<long[]>(0);

        Assert.NotSame(first, second);

        first[0] = 99;
        Assert.Equal(1L, reader.GetFieldValue<long[]>(0)[0]);
    }
}
