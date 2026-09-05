/**
 * This file is part of CamusDB
 *
 * Offline coverage for the NDJSON byte-level line framing (CamusDB.Client.Transport.Utf8LineReader).
 * The reader searches only bytes it has not examined yet, so the same body must frame identically no
 * matter how the stream fragments it — these tests drive the same bytes through 1-, 7-, 64- and
 * 4096-byte reads and compare the framing byte for byte, in both the sync and the async loop.
 */

using System.Text;
using CamusDB.Client.Transport;

namespace CamusDB.Client.Tests;

public class TestUtf8LineReader
{
    /// <summary>A stream that never returns more than <paramref name="chunk"/> bytes per read, the way a
    /// network stream delivers a large row in fragments.</summary>
    private sealed class FragmentStream(byte[] data, int chunk) : Stream
    {
        private int position;

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => data.Length;

        public override long Position
        {
            get => position;
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            int copied = Math.Min(Math.Min(chunk, count), data.Length - position);
            Array.Copy(data, position, buffer, offset, copied);
            position += copied;
            return copied;
        }

        public override int Read(Span<byte> buffer)
        {
            int copied = Math.Min(Math.Min(chunk, buffer.Length), data.Length - position);
            data.AsSpan(position, copied).CopyTo(buffer);
            position += copied;
            return copied;
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => ValueTask.FromResult(Read(buffer.Span));

        public override void Flush() { }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    private static List<string> FrameSync(byte[] data, int chunk)
    {
        using Utf8LineReader reader = new(new FragmentStream(data, chunk));
        List<string> lines = [];

        while (reader.ReadLine() is { } line)
            lines.Add(Encoding.UTF8.GetString(line.Span));

        return lines;
    }

    private static async Task<List<string>> FrameAsync(byte[] data, int chunk)
    {
        using Utf8LineReader reader = new(new FragmentStream(data, chunk));
        List<string> lines = [];

        while (await reader.ReadLineAsync(default) is { } line)
            lines.Add(Encoding.UTF8.GetString(line.Span));

        return lines;
    }

    // A body that exercises CRLF and LF framing, a blank line, multibyte UTF-8, a line long enough to
    // outgrow the initial buffer, and an unterminated final line.
    private static byte[] Body()
    {
        StringBuilder text = new();
        text.Append("first\n");
        text.Append("second\r\n");
        text.Append('\n');
        text.Append("café \U0001F600 中文\n");
        text.Append(new string('x', 20_000)).Append('\n');
        text.Append("{\"status\":\"ok\"}\n");
        text.Append("unterminated tail");

        return Encoding.UTF8.GetBytes(text.ToString());
    }

    private static List<string> Expected() =>
    [
        "first",
        "second",
        "",
        "café \U0001F600 中文",
        new('x', 20_000),
        "{\"status\":\"ok\"}",
        "unterminated tail",
    ];

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(64)]
    [InlineData(4096)]
    public void FragmentSizeDoesNotChangeSyncFraming(int chunk)
        => Assert.Equal(Expected(), FrameSync(Body(), chunk));

    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(64)]
    [InlineData(4096)]
    public async Task FragmentSizeDoesNotChangeAsyncFraming(int chunk)
        => Assert.Equal(Expected(), await FrameAsync(Body(), chunk));

    [Fact]
    public async Task SyncAndAsyncFrameIdentically()
    {
        byte[] body = Body();

        foreach (int chunk in new[] { 1, 3, 7, 64, 4096 })
            Assert.Equal(FrameSync(body, chunk), await FrameAsync(body, chunk));
    }

    [Fact]
    public void NewlineAtTheBufferBoundaryStillFrames()
    {
        // The initial capacity is 8192; place a newline exactly at that byte, so the line ends where the
        // buffer does and the next read has to make room first.
        string first = new('a', 8191);
        byte[] body = Encoding.UTF8.GetBytes(first + "\n" + "second\n");

        Assert.Equal([first, "second"], FrameSync(body, 1));
        Assert.Equal([first, "second"], FrameSync(body, 8192));
    }

    [Fact]
    public void MultibyteCharacterSplitAcrossReadsSurvives()
    {
        // One byte per read cuts every multibyte character apart; framing is by byte, so the decoded text
        // must still come back whole.
        byte[] body = Encoding.UTF8.GetBytes("áéí\U0001F600\n中文\n");

        Assert.Equal(["áéí\U0001F600", "中文"], FrameSync(body, 1));
    }

    [Fact]
    public void EmptyStreamYieldsNoLines()
        => Assert.Empty(FrameSync([], 1));

    [Fact]
    public void TrailingNewlineDoesNotProduceAnEmptyLine()
        => Assert.Equal(["only"], FrameSync(Encoding.UTF8.GetBytes("only\n"), 1));
}
