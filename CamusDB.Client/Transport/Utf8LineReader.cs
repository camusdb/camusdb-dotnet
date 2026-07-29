
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers;

namespace CamusDB.Client.Transport;

/// <summary>
/// Splits a byte stream into newline-delimited records without ever materializing them as UTF-16
/// strings — the allocation-free replacement for <see cref="StreamReader.ReadLine"/> on the NDJSON
/// streaming path, where a per-row <c>string</c> plus its UTF-8→UTF-16→UTF-8 round trip (the JSON
/// parser transcodes right back) dominates the per-row cost.
///
/// <para>Each returned line is a slice of the reader's internal pooled buffer, valid only until the
/// next <c>ReadLine</c> call — the caller must fully consume (or copy) it before advancing. A trailing
/// <c>'\r'</c> is trimmed so CRLF framing decodes like LF. A final unterminated line is returned at end
/// of stream, matching <see cref="StreamReader"/>.</para>
/// </summary>
internal sealed class Utf8LineReader(Stream stream, int initialCapacity = 8192) : IDisposable
{
    private byte[] buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
    private int start;    // first unconsumed byte
    private int end;      // one past the last valid byte
    private bool eof;

    /// <summary>Next line as a slice of the internal buffer, or null at end of stream.</summary>
    public ReadOnlyMemory<byte>? ReadLine()
    {
        while (true)
        {
            if (TryTakeBufferedLine(out ReadOnlyMemory<byte> line))
                return line;

            if (eof)
                return TakeRemainder();

            MakeRoom();
            int read = stream.Read(buffer, end, buffer.Length - end);
            if (read == 0)
                eof = true;
            else
                end += read;
        }
    }

    /// <inheritdoc cref="ReadLine"/>
    public async ValueTask<ReadOnlyMemory<byte>?> ReadLineAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            if (TryTakeBufferedLine(out ReadOnlyMemory<byte> line))
                return line;

            if (eof)
                return TakeRemainder();

            MakeRoom();
            int read = await stream.ReadAsync(buffer.AsMemory(end), cancellationToken).ConfigureAwait(false);
            if (read == 0)
                eof = true;
            else
                end += read;
        }
    }

    private bool TryTakeBufferedLine(out ReadOnlyMemory<byte> line)
    {
        int newline = Array.IndexOf(buffer, (byte)'\n', start, end - start);
        if (newline < 0)
        {
            line = default;
            return false;
        }

        line = Slice(start, newline);
        start = newline + 1;
        return true;
    }

    // The unterminated tail after the last newline, consumed once at end of stream.
    private ReadOnlyMemory<byte>? TakeRemainder()
    {
        if (start >= end)
            return null;

        ReadOnlyMemory<byte> line = Slice(start, end);
        start = end;
        return line;
    }

    private ReadOnlyMemory<byte> Slice(int from, int to)
    {
        if (to > from && buffer[to - 1] == (byte)'\r')
            to--;

        return buffer.AsMemory(from, to - from);
    }

    /// <summary>Ensures at least one writable byte past <see cref="end"/>: compacts consumed bytes to the
    /// front first, and only grows when a single line exceeds the whole buffer.</summary>
    private void MakeRoom()
    {
        if (end < buffer.Length)
            return;

        if (start > 0)
        {
            Buffer.BlockCopy(buffer, start, buffer, 0, end - start);
            end -= start;
            start = 0;
            return;
        }

        byte[] bigger = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
        Buffer.BlockCopy(buffer, 0, bigger, 0, end);
        ArrayPool<byte>.Shared.Return(buffer);
        buffer = bigger;
    }

    public void Dispose()
    {
        byte[] rented = buffer;
        buffer = [];
        start = end = 0;
        if (rented.Length > 0)
            ArrayPool<byte>.Shared.Return(rented);
    }
}
