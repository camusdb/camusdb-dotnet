/**
 * This file is part of CamusDB
 *
 * Offline coverage for the streaming (/execute-sql-query-stream) NDJSON reader
 * (CamusDB.Client.Transport.NdjsonStreamRowSource): schema-header parsing, incremental row decoding
 * (shared with the buffered decoder), HasRows via first-row prefetch, the empty result, and the in-band
 * failure trailer surfacing as a CamusException mid-read. Driven from an in-memory stream, so no server
 * is required.
 */

using System.Text;
using CamusDB.Client.Transport;

namespace CamusDB.Client.Tests;

public class TestNdjsonStreamRowSource
{
    private static async Task<CamusDataReader> ReaderFor(string ndjson)
    {
        MemoryStream stream = new(Encoding.UTF8.GetBytes(ndjson));
        NdjsonStreamRowSource source = await NdjsonStreamRowSource.CreateAsync(responseHandle: null, stream, default);
        return new CamusDataReader(source);
    }

    // type ints are the wire ColumnType: Id=1, Integer64=2, Bool=4.
    private const string Header = """{"status":"ok","columns":[{"name":"id","type":1},{"name":"n","type":2},{"name":"ok","type":4}]}""";

    [Fact]
    public async Task SchemaIsReadableBeforeAndWithoutRows()
    {
        // Header only, then an ok trailer, zero rows — the schema (names + types) must still be reported.
        await using CamusDataReader reader = await ReaderFor(
            Header + "\n" + """{"status":"ok","total":0,"serverTimeMs":0.4}""" + "\n");

        Assert.Equal(3, reader.FieldCount);
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("n", reader.GetName(1));
        Assert.Equal("ok", reader.GetName(2));
        Assert.False(reader.HasRows);
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task RowsDecodePositionallyAgainstSchema()
    {
        await using CamusDataReader reader = await ReaderFor(
            Header + "\n" +
            """["6849f3aa", 42, true]""" + "\n" +
            """["6849f3bb", 43, false]""" + "\n" +
            """{"status":"ok","total":2,"serverTimeMs":1.1}""" + "\n");

        Assert.True(reader.HasRows);

        Assert.True(await reader.ReadAsync());
        Assert.Equal("6849f3aa", reader.GetString(0));
        Assert.Equal(42L, reader.GetInt64(1));
        Assert.True(reader.GetBoolean(2));

        Assert.True(await reader.ReadAsync());
        Assert.Equal("6849f3bb", reader.GetString(0));
        Assert.Equal(43L, reader.GetInt64(1));
        Assert.False(reader.GetBoolean(2));

        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task SyncReadDrainsAllRows()
    {
        using CamusDataReader reader = await ReaderFor(
            Header + "\n" +
            """["a", 1, true]""" + "\n" +
            """["b", 2, true]""" + "\n" +
            """["c", 3, false]""" + "\n" +
            """{"status":"ok","total":3}""" + "\n");

        int count = 0;
        while (reader.Read())
            count++;

        Assert.Equal(3, count);
    }

    [Fact]
    public async Task FailureTrailerAfterRowsThrowsWhileReading()
    {
        await using CamusDataReader reader = await ReaderFor(
            Header + "\n" +
            """["a", 1, true]""" + "\n" +
            """{"status":"failed","total":1,"code":"CADB0502","message":"serializable conflict"}""" + "\n");

        // The first row reads cleanly...
        Assert.True(await reader.ReadAsync());
        Assert.Equal("a", reader.GetString(0));

        // ...then reaching the failure trailer surfaces the in-band error, exactly where the buffered call
        // would have thrown.
        CamusException ex = await Assert.ThrowsAsync<CamusException>(async () => await reader.ReadAsync());
        Assert.Equal("CADB0502", ex.Code);
        Assert.Contains("conflict", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TruncatedStreamAfterRowsEndsCleanly()
    {
        // No trailer at all (connection cut after a row) — treated as end of result, not an error.
        await using CamusDataReader reader = await ReaderFor(Header + "\n" + """["a", 1, true]""" + "\n");

        Assert.True(await reader.ReadAsync());
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task MissingHeaderThrows()
    {
        MemoryStream stream = new(Encoding.UTF8.GetBytes(""));
        await Assert.ThrowsAsync<CamusException>(
            async () => await NdjsonStreamRowSource.CreateAsync(responseHandle: null, stream, default));
    }

    [Fact]
    public async Task DisposeReleasesTheUnderlyingResponse()
    {
        DisposeProbe probe = new();
        MemoryStream stream = new(Encoding.UTF8.GetBytes(Header + "\n" + """{"status":"ok","total":0}""" + "\n"));
        NdjsonStreamRowSource source = await NdjsonStreamRowSource.CreateAsync(probe, stream, default);

        await source.DisposeAsync();

        Assert.True(probe.Disposed);
    }

    private sealed class DisposeProbe : IDisposable
    {
        public bool Disposed { get; private set; }
        public void Dispose() => Disposed = true;
    }
}
