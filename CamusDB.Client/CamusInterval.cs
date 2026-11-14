/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;

namespace CamusDB.Client;

/// <summary>
/// One SQL <c>interval</c> value: a number of months, a number of days and a number of microseconds.
/// The server keeps the three parts separate, as PostgreSQL does, because a month and a day have no
/// fixed length when they are added to a date.
///
/// <para><b>On the wire.</b> An interval is a value of an expression only: no table column can have
/// this type. A query result sends it as its ISO-8601 text (<c>P1Y2M3DT4H5M6.5S</c>) in a
/// <c>string</c> column. A parameter is text too, so the SQL must cast it:
/// <c>CAST(@p AS interval)</c>. A <see cref="CamusInterval"/> or a <see cref="TimeSpan"/> parameter
/// binds as this text.</para>
///
/// <para><b>TimeSpan.</b> A <see cref="TimeSpan"/> has days and a time, and no months. So
/// <see cref="ToTimeSpan"/> refuses an interval with a month part. Read such a value, for example
/// the result of <c>age(a, b)</c>, as a <see cref="CamusInterval"/>.</para>
///
/// <para><b>Equality.</b> Two values are equal when their three parts are equal. The server compares
/// two intervals by one total, where a month is 30 days and a day is 24 hours, so on the server
/// <c>INTERVAL 'P1M' = INTERVAL 'P30D'</c> is true. Here the two values are not equal.</para>
/// </summary>
public readonly struct CamusInterval : IEquatable<CamusInterval>
{
    private const long MicrosPerSecond = 1_000_000;
    private const long MicrosPerMinute = 60 * MicrosPerSecond;
    private const long MicrosPerHour = 60 * MicrosPerMinute;
    private const long MicrosPerDay = 24 * MicrosPerHour;

    /// <summary>Creates an interval from its three parts.</summary>
    public CamusInterval(int months, int days, long microseconds)
    {
        Months = months;
        Days = days;
        Microseconds = microseconds;
    }

    /// <summary>The months part. A year is 12 months.</summary>
    public int Months { get; }

    /// <summary>The days part.</summary>
    public int Days { get; }

    /// <summary>The time part, in microseconds.</summary>
    public long Microseconds { get; }

    /// <summary>
    /// The interval of a <see cref="TimeSpan"/>: its whole days in the days part, and the rest in the
    /// time part. The time part rounds to a whole microsecond, to even on a tie, as the server rounds
    /// the text of a parameter.
    /// </summary>
    public static CamusInterval FromTimeSpan(TimeSpan value)
    {
        long remainderTicks = value.Ticks % TimeSpan.TicksPerDay;
        long micros = (long)Math.Round(remainderTicks / (decimal)TimeSpan.TicksPerMicrosecond, MidpointRounding.ToEven);

        return new CamusInterval(0, value.Days, micros);
    }

    /// <summary>
    /// The <see cref="TimeSpan"/> of this interval: the days times 24 hours, plus the time part.
    /// </summary>
    /// <exception cref="InvalidOperationException">The interval has a month part, which a
    /// <see cref="TimeSpan"/> cannot hold.</exception>
    /// <exception cref="OverflowException">The value is outside the range of a <see cref="TimeSpan"/>.</exception>
    public TimeSpan ToTimeSpan()
    {
        if (Months != 0)
            throw new InvalidOperationException(
                $"The interval '{this}' has a month part, and a TimeSpan has no months. " +
                "Read the value as a CamusInterval, or remove the months in SQL with justify_days or a datetime difference.");

        return new TimeSpan(checked(Days * TimeSpan.TicksPerDay + Microseconds * TimeSpan.TicksPerMicrosecond));
    }

    /// <summary>
    /// Reads the ISO-8601 text of an interval, the form the server sends: <c>P1Y2M3DT4H5M6.5S</c>,
    /// <c>P-1DT2H</c>, <c>PT0S</c>. Each number can have its own sign. Only the seconds can have a
    /// fraction, which rounds to a whole microsecond.
    /// </summary>
    /// <exception cref="FormatException">The text is not an ISO-8601 interval.</exception>
    /// <exception cref="OverflowException">A part is outside its range.</exception>
    public static CamusInterval Parse(string text)
    {
        ArgumentNullException.ThrowIfNull(text);

        return TryParseCore(text, out CamusInterval value, out bool overflow)
            ? value
            : overflow
                ? throw new OverflowException($"The interval '{text}' is outside the range of an interval.")
                : throw new FormatException($"'{text}' is not an ISO-8601 interval, such as P1DT2H or PT0S.");
    }

    /// <summary>Reads the ISO-8601 text of an interval. See <see cref="Parse"/>.</summary>
    public static bool TryParse(string? text, out CamusInterval value)
    {
        value = default;

        return text is not null && TryParseCore(text, out value, out _);
    }

    private static bool TryParseCore(string text, out CamusInterval value, out bool overflow)
    {
        value = default;
        overflow = false;

        ReadOnlySpan<char> span = text.AsSpan().Trim();

        if (span.Length < 2 || span[0] is not ('P' or 'p'))
            return false;

        decimal months = 0, days = 0, micros = 0;
        bool timePart = false, any = false, anyTime = false;
        int i = 1;

        while (i < span.Length)
        {
            if (span[i] is 'T' or 't')
            {
                if (timePart)
                    return false;

                timePart = true;
                i++;
                continue;
            }

            int start = i;

            if (span[i] is '+' or '-')
                i++;

            while (i < span.Length && (char.IsAsciiDigit(span[i]) || span[i] == '.'))
                i++;

            if (i >= span.Length || !decimal.TryParse(span[start..i], NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out decimal number))
                return false;

            char designator = char.ToUpperInvariant(span[i++]);
            bool whole = decimal.Truncate(number) == number;

            try
            {
                switch (designator, timePart)
                {
                    case ('Y', false) when whole: months += number * 12; break;
                    case ('M', false) when whole: months += number; break;
                    case ('W', false) when whole: days += number * 7; break;
                    case ('D', false) when whole: days += number; break;
                    case ('H', true) when whole: micros += number * MicrosPerHour; break;
                    case ('M', true) when whole: micros += number * MicrosPerMinute; break;
                    case ('S', true): micros += number * MicrosPerSecond; break;
                    default: return false;
                }
            }
            catch (OverflowException)
            {
                overflow = true;
                return false;
            }

            any = true;
            anyTime |= timePart;
        }

        // A 'T' must have at least one time designator after it.
        if (!any || (timePart && !anyTime))
            return false;

        micros = Math.Round(micros, MidpointRounding.ToEven);

        if (months < int.MinValue || months > int.MaxValue
            || days < int.MinValue || days > int.MaxValue
            || micros < long.MinValue || micros > long.MaxValue)
        {
            overflow = true;
            return false;
        }

        value = new CamusInterval((int)months, (int)days, (long)micros);
        return true;
    }

    /// <summary>
    /// The ISO-8601 text of this interval, in the form the server writes: the years and the months
    /// share the sign of the months part, and the hours, minutes and seconds share the sign of the
    /// time part. A zero interval is <c>PT0S</c>.
    /// </summary>
    public override string ToString()
    {
        if (Months == 0 && Days == 0 && Microseconds == 0)
            return "PT0S";

        StringBuilder text = new(32);
        text.Append('P');

        int years = Months / 12;
        int months = Months % 12;

        if (years != 0)
            text.Append(years.ToString(CultureInfo.InvariantCulture)).Append('Y');

        if (months != 0)
            text.Append(months.ToString(CultureInfo.InvariantCulture)).Append('M');

        if (Days != 0)
            text.Append(Days.ToString(CultureInfo.InvariantCulture)).Append('D');

        if (Microseconds != 0)
        {
            long micros = Microseconds;
            long hours = micros / MicrosPerHour;
            micros -= hours * MicrosPerHour;
            long minutes = micros / MicrosPerMinute;
            micros -= minutes * MicrosPerMinute;
            long seconds = micros / MicrosPerSecond;
            long fraction = micros - seconds * MicrosPerSecond;

            text.Append('T');

            if (hours != 0)
                text.Append(hours.ToString(CultureInfo.InvariantCulture)).Append('H');

            if (minutes != 0)
                text.Append(minutes.ToString(CultureInfo.InvariantCulture)).Append('M');

            if (seconds != 0 || fraction != 0)
            {
                if (seconds < 0 || fraction < 0)
                    text.Append('-');

                text.Append(Math.Abs(seconds).ToString(CultureInfo.InvariantCulture));

                if (fraction != 0)
                    text.Append('.').Append(Math.Abs(fraction).ToString("D6", CultureInfo.InvariantCulture).TrimEnd('0'));

                text.Append('S');
            }
        }

        return text.ToString();
    }

    /// <summary>
    /// The text of a <see cref="TimeSpan"/> or a <see cref="CamusInterval"/> parameter. A
    /// <see cref="TimeSpan"/> keeps every tick, and the server rounds the seconds to a microsecond.
    /// </summary>
    internal static string ToParameterText(object value) => value switch
    {
        CamusInterval interval => interval.ToString(),
        TimeSpan span => FormatTimeSpan(span),
        _ => throw new ArgumentException($"{value.GetType().Name} is not an interval value.", nameof(value)),
    };

    /// <summary>
    /// The ISO-8601 text of a <see cref="TimeSpan"/>, with the whole days in the days part and the rest
    /// in the time part. The fraction of a second keeps all seven digits of a tick.
    /// </summary>
    private static string FormatTimeSpan(TimeSpan value)
    {
        long ticks = value.Ticks % TimeSpan.TicksPerDay;

        if (ticks % TimeSpan.TicksPerMicrosecond == 0)
            return new CamusInterval(0, value.Days, ticks / TimeSpan.TicksPerMicrosecond).ToString();

        // A tick is a tenth of a microsecond. Write the whole microseconds, then the last digit.
        string text = new CamusInterval(0, value.Days, 0).ToString();
        StringBuilder builder = new(text == "PT0S" ? "P" : text, 40);
        builder.Append('T');

        long hours = ticks / TimeSpan.TicksPerHour;
        ticks -= hours * TimeSpan.TicksPerHour;
        long minutes = ticks / TimeSpan.TicksPerMinute;
        ticks -= minutes * TimeSpan.TicksPerMinute;
        long seconds = ticks / TimeSpan.TicksPerSecond;
        long fraction = ticks - seconds * TimeSpan.TicksPerSecond;

        if (hours != 0)
            builder.Append(hours.ToString(CultureInfo.InvariantCulture)).Append('H');

        if (minutes != 0)
            builder.Append(minutes.ToString(CultureInfo.InvariantCulture)).Append('M');

        if (seconds < 0 || fraction < 0)
            builder.Append('-');

        builder.Append(Math.Abs(seconds).ToString(CultureInfo.InvariantCulture))
            .Append('.')
            .Append(Math.Abs(fraction).ToString("D7", CultureInfo.InvariantCulture).TrimEnd('0'))
            .Append('S');

        return builder.ToString();
    }

    /// <inheritdoc />
    public bool Equals(CamusInterval other)
        => Months == other.Months && Days == other.Days && Microseconds == other.Microseconds;

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is CamusInterval other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(Months, Days, Microseconds);

    public static bool operator ==(CamusInterval left, CamusInterval right) => left.Equals(right);

    public static bool operator !=(CamusInterval left, CamusInterval right) => !left.Equals(right);
}
