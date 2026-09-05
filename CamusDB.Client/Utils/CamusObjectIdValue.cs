
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;

namespace CamusDB.Core.Util.ObjectIds;

public record struct CamusObjectIdValue : IComparable<CamusObjectIdValue>
{
    public int a;

    public int b;

    public int c;

    public CamusObjectIdValue(int a, int b, int c)
    {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public byte[] ToBytes()
    {
        byte[] buffer = new byte[12];

        buffer[0] = (byte)((a >> 0) & 0xff);
        buffer[1] = (byte)((a >> 8) & 0xff);
        buffer[2] = (byte)((a >> 16) & 0xff);
        buffer[3] = (byte)((a >> 24) & 0xff);

        buffer[4] = (byte)((b >> 0) & 0xff);
        buffer[5] = (byte)((b >> 8) & 0xff);
        buffer[6] = (byte)((b >> 16) & 0xff);
        buffer[7] = (byte)((b >> 24) & 0xff);

        buffer[8] = (byte)((c >> 0) & 0xff);
        buffer[9] = (byte)((c >> 8) & 0xff);
        buffer[10] = (byte)((c >> 16) & 0xff);
        buffer[11] = (byte)((c >> 24) & 0xff);

        return buffer;
    }

    public bool IsNull()
    {
        return a == 0 && b == 0 && c == 0;
    }

    /// <summary>
    /// Orders two ids by their 12 bytes, most significant first — which orders them by creation time,
    /// since the first four bytes are the timestamp.
    ///
    /// <para>The comparison is unsigned. The three fields are the raw bytes reinterpreted as
    /// <see cref="int"/>, so a signed comparison would sort any value whose top bit is set below every
    /// other one. Returning a constant 1 for "not equal", as this did before, is not an ordering at all:
    /// it makes <c>a &lt; b</c> and <c>b &lt; a</c> both true, which leaves a sort producing an arbitrary
    /// order and can make <see cref="List{T}.Sort"/> throw for an inconsistent comparer.</para>
    /// </summary>
    public int CompareTo(CamusObjectIdValue other)
    {
        int comparison = ((uint)a).CompareTo((uint)other.a);
        if (comparison != 0)
            return comparison;

        comparison = ((uint)b).CompareTo((uint)other.b);
        if (comparison != 0)
            return comparison;

        return ((uint)c).CompareTo((uint)other.c);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static char ToHexChar(int value)
    {
        return (char)(value + (value < 10 ? '0' : 'a' - 10));
    }

    public override string ToString()
    {
        return ToString(a, b, c);
    }

    public static string ToString(int _a, int _b, int _c)
    {
        // Write the 24 hex chars straight into the string's backing buffer — no intermediate char[].
        return string.Create(24, (_a, _b, _c), static (span, state) =>
        {
            (int a, int b, int c) = state;
            WriteHex(span[..8], a);
            WriteHex(span.Slice(8, 8), b);
            WriteHex(span.Slice(16, 8), c);
        });
    }

    private static void WriteHex(Span<char> span, int value)
    {
        span[0] = ToHexChar((value >> 28) & 0x0f);
        span[1] = ToHexChar((value >> 24) & 0x0f);
        span[2] = ToHexChar((value >> 20) & 0x0f);
        span[3] = ToHexChar((value >> 16) & 0x0f);
        span[4] = ToHexChar((value >> 12) & 0x0f);
        span[5] = ToHexChar((value >> 8) & 0x0f);
        span[6] = ToHexChar((value >> 4) & 0x0f);
        span[7] = ToHexChar(value & 0x0f);
    }

    private static bool TryParseHexChar(char c, out int value)
    {
        if (c >= '0' && c <= '9')
        {
            value = c - '0';
            return true;
        }

        if (c >= 'a' && c <= 'f')
        {
            value = 10 + (c - 'a');
            return true;
        }

        if (c >= 'A' && c <= 'F')
        {
            value = 10 + (c - 'A');
            return true;
        }

        value = 0;
        return false;
    }

    public static bool TryParseHexString(string s, out byte[] bytes)
    {
        bytes = Array.Empty<byte>();

        if (s == null)
            return false;

        byte[] buffer = new byte[(s.Length + 1) / 2];

        int i = 0;
        int j = 0;

        if ((s.Length % 2) == 1) // if s has an odd length assume an implied leading "0"
        {
            if (!TryParseHexChar(s[i++], out int y))
                return false;

            buffer[j++] = (byte)y;
        }

        while (i < s.Length)
        {
            if (!TryParseHexChar(s[i++], out int x))
                return false;

            if (!TryParseHexChar(s[i++], out int y))
                return false;

            buffer[j++] = (byte)((x << 4) | y);
        }

        bytes = buffer;
        return true;
    }

    /// <summary>
    /// Parses the 24-character hexadecimal form. The length is checked before the bytes are read: a
    /// shorter string used to index past the end of the parsed buffer and surface as an
    /// <see cref="IndexOutOfRangeException"/> rather than as a parse failure.
    /// </summary>
    public static CamusObjectIdValue ToValue(string s)
    {
        if (!TryParseHexString(s, out byte[] bytes) || bytes.Length != 12)
            throw new FormatException("An ObjectId is 24 hexadecimal digits.");

        int a = (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
        int b = (bytes[4] << 24) | (bytes[5] << 16) | (bytes[6] << 8) | bytes[7];
        int c = (bytes[8] << 24) | (bytes[9] << 16) | (bytes[10] << 8) | bytes[11];

        return new(a, b, c);
    }
}
