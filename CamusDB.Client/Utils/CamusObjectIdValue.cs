
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

    public int CompareTo(CamusObjectIdValue other)
    {
        if (this.a == other.a && this.b == other.b && this.c == other.c)
            return 0;

        return 1;
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
        char[] c = new char[24];

        c[0] = ToHexChar((_a >> 28) & 0x0f);
        c[1] = ToHexChar((_a >> 24) & 0x0f);
        c[2] = ToHexChar((_a >> 20) & 0x0f);
        c[3] = ToHexChar((_a >> 16) & 0x0f);
        c[4] = ToHexChar((_a >> 12) & 0x0f);
        c[5] = ToHexChar((_a >> 8) & 0x0f);
        c[6] = ToHexChar((_a >> 4) & 0x0f);
        c[7] = ToHexChar(_a & 0x0f);

        c[8] = ToHexChar((_b >> 28) & 0x0f);
        c[9] = ToHexChar((_b >> 24) & 0x0f);
        c[10] = ToHexChar((_b >> 20) & 0x0f);
        c[11] = ToHexChar((_b >> 16) & 0x0f);
        c[12] = ToHexChar((_b >> 12) & 0x0f);
        c[13] = ToHexChar((_b >> 8) & 0x0f);
        c[14] = ToHexChar((_b >> 4) & 0x0f);
        c[15] = ToHexChar(_b & 0x0f);

        c[16] = ToHexChar((_c >> 28) & 0x0f);
        c[17] = ToHexChar((_c >> 24) & 0x0f);
        c[18] = ToHexChar((_c >> 20) & 0x0f);
        c[19] = ToHexChar((_c >> 16) & 0x0f);
        c[20] = ToHexChar((_c >> 12) & 0x0f);
        c[21] = ToHexChar((_c >> 8) & 0x0f);
        c[22] = ToHexChar((_c >> 4) & 0x0f);
        c[23] = ToHexChar(_c & 0x0f);

        return new string(c);
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

    public static CamusObjectIdValue ToValue(string s)
    {
        if (!TryParseHexString(s, out byte[] bytes))
            throw new FormatException("String should contain only hexadecimal digits.");

        int a = (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3];
        int b = (bytes[4] << 24) | (bytes[5] << 16) | (bytes[6] << 8) | bytes[7];
        int c = (bytes[8] << 24) | (bytes[9] << 16) | (bytes[10] << 8) | bytes[11];

        return new(a, b, c);
    }
}
