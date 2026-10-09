
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;

namespace CamusDB.Client;

/// <summary>
/// Conversions between a CamusDB <see cref="ColumnType.Numeric"/> value and .NET types.
///
/// <para>A NUMERIC holds 38 significant digits, 9 of them after the point, from
/// <c>-99999999999999999999999999999.999999999</c> to <c>99999999999999999999999999999.999999999</c>.
/// It travels as decimal text on every transport. The server sends the canonical form: no exponent, no
/// trailing zeros and no point for a whole number (<c>"1.5"</c>, <c>"1200"</c>, <c>"0"</c>).</para>
///
/// <para>A <see cref="decimal"/> holds 28 or 29 significant digits and a magnitude below about
/// 7.9 × 10²⁸, so it cannot hold every NUMERIC. A read never rounds: a value that a <see cref="decimal"/>
/// cannot hold exactly fails, and <see cref="CamusDataReader.GetString"/> gives its exact text.</para>
/// </summary>
public static class CamusNumeric
{
    /// <summary>
    /// The text that binds <paramref name="value"/> with every digit. The server rounds more than 9
    /// fraction digits half away from zero.
    /// </summary>
    public static string ToText(decimal value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>
    /// Reads NUMERIC text as a <see cref="decimal"/>. Returns false when the text is not a number, or when
    /// a <see cref="decimal"/> cannot hold the value exactly. A value is never rounded.
    /// </summary>
    public static bool TryToDecimal(string? text, out decimal value)
    {
        value = 0;

        if (string.IsNullOrEmpty(text))
            return false;

        if (!decimal.TryParse(text, NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                CultureInfo.InvariantCulture, out decimal parsed))
            return false;

        // decimal.TryParse rounds the digits it cannot hold, with no error. A parsed decimal keeps the
        // scale of its text, so the two spellings agree, trailing zeros aside, exactly when no digit
        // was rounded.
        if (!TrimFraction(parsed.ToString(CultureInfo.InvariantCulture)).SequenceEqual(TrimFraction(TrimSign(text))))
            return false;

        value = parsed;
        return true;
    }

    /// <summary>
    /// Reads NUMERIC text as a <see cref="decimal"/>, or throws <see cref="InvalidCastException"/> when a
    /// <see cref="decimal"/> cannot hold the value exactly.
    /// </summary>
    public static decimal ToDecimal(string? text)
    {
        if (TryToDecimal(text, out decimal value))
            return value;

        throw new InvalidCastException(
            $"The NUMERIC value '{text}' does not fit in a System.Decimal without rounding. " +
            "Read it with GetString to get its exact text.");
    }

    /// <summary>
    /// Reads NUMERIC text as a <see cref="double"/>, rounded to the nearest double.
    /// </summary>
    public static double ToDouble(string? text) => double.Parse(text ?? "", NumberStyles.Float, CultureInfo.InvariantCulture);

    /// <summary>
    /// The text that binds a parameter value as NUMERIC. A <see cref="string"/> passes through as it is,
    /// so a value wider than a <see cref="decimal"/> can bind exactly; the server validates it.
    /// </summary>
    internal static string FromParameter(string name, object value) => value switch
    {
        decimal m => ToText(m),
        string s => s,
        long or int or short or sbyte or ulong or uint or ushort or byte
            => ((IFormattable)value).ToString(null, CultureInfo.InvariantCulture),
        double d => FromDouble(name, d),
        float f => FromDouble(name, f),
        _ => throw new CamusException("CADB0400", $"Cannot map NUMERIC parameter '{name}' from {value.GetType().Name}")
    };

    // "R" gives the shortest text that parses back to the same double, so 0.1 binds as NUMERIC 0.1 and
    // not as its exact binary value. An exponent ("1E+20") is valid NUMERIC text.
    private static string FromDouble(string name, double value)
    {
        if (!double.IsFinite(value))
            throw new CamusException("CADB0400", $"NUMERIC parameter '{name}' cannot hold {value.ToString(CultureInfo.InvariantCulture)}");

        return value.ToString("R", CultureInfo.InvariantCulture);
    }

    private static ReadOnlySpan<char> TrimSign(string text) => text.Length > 0 && text[0] == '+' ? text.AsSpan(1) : text;

    private static ReadOnlySpan<char> TrimFraction(ReadOnlySpan<char> text)
    {
        if (text.IndexOf('.') < 0)
            return text;

        return text.TrimEnd('0').TrimEnd('.');
    }
}
