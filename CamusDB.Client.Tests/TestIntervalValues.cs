/**
 * This file is part of CamusDB
 *
 * Offline coverage for the interval support: the ISO-8601 text in CamusInterval, the TimeSpan
 * conversions, parameter binding, the data reader and the Dapper handler. The server sends an interval
 * as its ISO-8601 text in a string column, and reads a parameter as text with CAST(@p AS interval).
 * No server is required.
 */

using System.Data;
using System.Text.Json;
using CamusDB.Client.Transport;
using CamusDB.Dapper;

namespace CamusDB.Client.Tests;

public class TestIntervalValues
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private const long MicrosPerHour = 3_600_000_000;

    // ─── CamusInterval text ───────────────────────────────────────────────────

    // Each pair is the server's FormatIso output for the parts, checked on the live server.
    [Theory]
    [InlineData("PT0S", 0, 0, 0L)]
    [InlineData("P1DT2H", 0, 1, 2 * MicrosPerHour)]
    [InlineData("P1DT15H", 0, 1, 15 * MicrosPerHour)]
    [InlineData("P-7D", 0, -7, 0L)]
    [InlineData("P43Y9M27D", 525, 27, 0L)]
    [InlineData("P1Y2M3DT4H5M6.5S", 14, 3, 4 * MicrosPerHour + 5 * 60_000_000L + 6_500_000L)]
    [InlineData("P-1DT-12H", 0, -1, -12 * MicrosPerHour)]
    [InlineData("PT-1H-30M-5.25S", 0, 0, -(MicrosPerHour + 30 * 60_000_000L + 5_250_000L))]
    [InlineData("P1DT2H3M4.123457S", 0, 1, 2 * MicrosPerHour + 3 * 60_000_000L + 4_123_457L)]
    [InlineData("PT-0.000001S", 0, 0, -1L)]
    [InlineData("P-1Y-2M", -14, 0, 0L)]
    public void ServerTextRoundTrips(string text, int months, int days, long micros)
    {
        CamusInterval value = CamusInterval.Parse(text);

        Assert.Equal(new CamusInterval(months, days, micros), value);
        Assert.Equal(text, value.ToString());
    }

    [Theory]
    [InlineData("P2W", 0, 14, 0L)]
    [InlineData("p1dt2h", 0, 1, 2 * MicrosPerHour)]
    [InlineData(" PT1.0000005S ", 0, 0, 1_000_000L)]
    [InlineData("PT1.0000015S", 0, 0, 1_000_002L)]
    [InlineData("P+1D", 0, 1, 0L)]
    public void ParseReadsOtherIsoForms(string text, int months, int days, long micros)
    {
        Assert.Equal(new CamusInterval(months, days, micros), CamusInterval.Parse(text));
    }

    [Theory]
    [InlineData("")]
    [InlineData("P")]
    [InlineData("PT")]
    [InlineData("P1DT")]
    [InlineData("1 day")]
    [InlineData("01:00:00")]
    [InlineData("P1.5D")]
    [InlineData("P1H")]
    [InlineData("PT1D")]
    [InlineData("P1DT2H3")]
    [InlineData("PTT1H")]
    public void ParseRefusesTextThatIsNotAnIsoInterval(string text)
    {
        Assert.False(CamusInterval.TryParse(text, out _));
        Assert.Throws<FormatException>(() => CamusInterval.Parse(text));
    }

    [Fact]
    public void ParseRefusesAPartOutOfRange()
    {
        Assert.Throws<OverflowException>(() => CamusInterval.Parse("P9999999999D"));
        Assert.False(CamusInterval.TryParse(null, out _));
    }

    [Fact]
    public void EqualityComparesTheThreeParts()
    {
        // On the server INTERVAL 'P1M' = INTERVAL 'P30D' is true. Here the parts differ.
        Assert.NotEqual(new CamusInterval(1, 0, 0), new CamusInterval(0, 30, 0));
        Assert.True(new CamusInterval(1, 2, 3) == CamusInterval.Parse("P1M2DT0.000003S"));
        Assert.Equal(new CamusInterval(1, 2, 3).GetHashCode(), new CamusInterval(1, 2, 3).GetHashCode());
    }

    // ─── TimeSpan ─────────────────────────────────────────────────────────────

    [Fact]
    public void TimeSpanConvertsWithItsDaysInTheDaysPart()
    {
        TimeSpan span = new(1, 2, 3, 4, 500);

        CamusInterval interval = CamusInterval.FromTimeSpan(span);

        Assert.Equal(new CamusInterval(0, 1, 2 * MicrosPerHour + 3 * 60_000_000L + 4_500_000L), interval);
        Assert.Equal("P1DT2H3M4.5S", interval.ToString());
        Assert.Equal(span, interval.ToTimeSpan());

        TimeSpan negative = -TimeSpan.FromHours(36);
        Assert.Equal("P-1DT-12H", CamusInterval.FromTimeSpan(negative).ToString());
        Assert.Equal(negative, CamusInterval.Parse("P-1DT-12H").ToTimeSpan());
    }

    [Fact]
    public void FromTimeSpanRoundsATickToAMicrosecondToEven()
    {
        Assert.Equal(0, CamusInterval.FromTimeSpan(TimeSpan.FromTicks(5)).Microseconds);
        Assert.Equal(2, CamusInterval.FromTimeSpan(TimeSpan.FromTicks(15)).Microseconds);
        Assert.Equal(-2, CamusInterval.FromTimeSpan(TimeSpan.FromTicks(-15)).Microseconds);
    }

    [Fact]
    public void ToTimeSpanRefusesAMonthPart()
    {
        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() => CamusInterval.Parse("P1M").ToTimeSpan());
        Assert.Contains("CamusInterval", ex.Message);
    }

    [Fact]
    public void ToTimeSpanRefusesAValueOutOfRange()
    {
        Assert.Throws<OverflowException>(() => new CamusInterval(0, 0, long.MaxValue).ToTimeSpan());
    }

    // ─── Parameter binding ────────────────────────────────────────────────────

    private static ColumnValue Bind(object? value, ColumnType columnType)
    {
        CamusCommand command = new("SELECT 1", new CamusConnectionStringBuilder(ConnString));
        command.Parameters.Add("@p", columnType, value);

        return command.GetCommandParameters(CamusProtocol.Rest)!["@p"];
    }

    private static ColumnValue BindByDbType(object? value, DbType dbType)
    {
        CamusCommand command = new("SELECT 1", new CamusConnectionStringBuilder(ConnString));
        command.Parameters.Add(new CamusParameter { ParameterName = "@p", DbType = dbType, Value = value });

        return command.GetCommandParameters(CamusProtocol.Rest)!["@p"];
    }

    [Fact]
    public void UntypedTimeSpanBindsAsIntervalText()
    {
        ColumnValue value = Bind(new TimeSpan(1, 2, 0, 0), ColumnType.Null);

        Assert.Equal(ColumnType.String, value.Type);
        Assert.Equal("P1DT2H", value.StrValue);
    }

    [Fact]
    public void TimeSpanKeepsEveryTickInItsText()
    {
        // The server rounds the seconds to a microsecond, as it does for any interval text.
        Assert.Equal("PT1.0000005S", Bind(TimeSpan.FromTicks(10_000_005), ColumnType.Null).StrValue);
        Assert.Equal("P-1DT-1H-0.0000001S", Bind(-(TimeSpan.FromHours(25) + TimeSpan.FromTicks(1)), ColumnType.Null).StrValue);
        Assert.Equal("PT0.0000001S", Bind(TimeSpan.FromTicks(1), ColumnType.Null).StrValue);
        Assert.Equal("P2DT0.0000003S", Bind(TimeSpan.FromDays(2) + TimeSpan.FromTicks(3), ColumnType.Null).StrValue);
        Assert.Equal("PT0S", Bind(TimeSpan.Zero, ColumnType.Null).StrValue);
    }

    [Theory]
    [InlineData(DbType.Time)]
    [InlineData(DbType.String)]
    public void TimeSpanWithAStringDbTypeBindsAsIntervalText(DbType dbType)
    {
        // Dapper binds a TimeSpan as DbType.Time, which maps to a string parameter.
        ColumnValue value = BindByDbType(TimeSpan.FromMinutes(90), dbType);

        Assert.Equal(ColumnType.String, value.Type);
        Assert.Equal("PT1H30M", value.StrValue);
    }

    [Fact]
    public void CamusIntervalBindsAsItsText()
    {
        ColumnValue value = Bind(new CamusInterval(14, 3, 0), ColumnType.Null);

        Assert.Equal(ColumnType.String, value.Type);
        Assert.Equal("P1Y2M3D", value.StrValue);
    }

    [Fact]
    public void TimeSpanArrayNeedsAnExplicitElementType()
    {
        // An interval cannot be an array element on the server, so no element type is inferred.
        List<TimeSpan> spans = [TimeSpan.FromHours(1), TimeSpan.FromDays(1)];

        CamusException ex = Assert.Throws<CamusException>(() => Bind(spans, ColumnType.Array));
        Assert.Equal("CADB0400", ex.Code);

        CamusCommand command = new("SELECT 1", new CamusConnectionStringBuilder(ConnString));
        command.Parameters.Add(new CamusParameter("@p", ColumnType.Array, spans) { ArrayElementType = ColumnType.String });
        ColumnValue array = command.GetCommandParameters(CamusProtocol.Rest)!["@p"];

        Assert.Equal(ColumnType.String, array.ArrayElementType);
        Assert.Equal(["PT1H", "P1D"], array.ArrayValues!.Select(e => e.StrValue));
    }

    [Fact]
    public void NullTimeSpanBindsAsNull()
    {
        Assert.Equal(ColumnType.Null, Bind(null, ColumnType.Null).Type);
        Assert.Equal(ColumnType.Null, Bind(DBNull.Value, ColumnType.String).Type);
    }

    // ─── Reader ───────────────────────────────────────────────────────────────

    private static CamusDataReader ReaderFor(string rows, ColumnType declared = ColumnType.String)
    {
        List<CamusColumnSchema> columns = [new() { Name = "d", Type = declared }];

        using JsonDocument doc = JsonDocument.Parse(rows);
        return new CamusDataReader(CamusResultSet.FromWire(columns, doc.RootElement.Clone()));
    }

    [Fact]
    public void IntervalTextReadsAsATimeSpan()
    {
        using CamusDataReader reader = ReaderFor("""[["P1DT2H3M4.123457S"], ["P-1DT-12H"], ["PT0S"], [null]]""");

        Assert.Equal(typeof(string), reader.GetFieldType(0));

        Assert.True(reader.Read());
        TimeSpan expected = new TimeSpan(1, 2, 3, 4) + TimeSpan.FromTicks(1_234_570);
        Assert.Equal(expected, reader.GetTimeSpan(0));
        Assert.Equal(expected, reader.GetFieldValue<TimeSpan>(0));
        Assert.Equal("P1DT2H3M4.123457S", reader.GetString(0));
        Assert.Equal("P1DT2H3M4.123457S", reader.GetValue(0));

        Assert.True(reader.Read());
        Assert.Equal(-TimeSpan.FromHours(36), reader.GetFieldValue<TimeSpan>(0));

        Assert.True(reader.Read());
        Assert.Equal(TimeSpan.Zero, reader.GetTimeSpan(0));

        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(0));
        Assert.Throws<InvalidCastException>(() => reader.GetTimeSpan(0));
    }

    [Fact]
    public void IntervalWithMonthsReadsAsACamusIntervalOnly()
    {
        using CamusDataReader reader = ReaderFor("""[["P43Y9M27D"]]""");

        Assert.True(reader.Read());
        Assert.Equal(new CamusInterval(525, 27, 0), reader.GetInterval(0));
        Assert.Equal(new CamusInterval(525, 27, 0), reader.GetFieldValue<CamusInterval>(0));

        InvalidCastException ex = Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<TimeSpan>(0));
        Assert.Contains("'d'", ex.Message);
        Assert.Contains("month", ex.Message);
    }

    [Fact]
    public void DotNetTimeSpanTextReadsAsATimeSpan()
    {
        using CamusDataReader reader = ReaderFor("""[["1.02:03:04"], ["not a span"]]""");

        Assert.True(reader.Read());
        Assert.Equal(new TimeSpan(1, 2, 3, 4), reader.GetTimeSpan(0));
        Assert.Throws<InvalidCastException>(() => reader.GetInterval(0));

        Assert.True(reader.Read());
        Assert.Throws<InvalidCastException>(() => reader.GetTimeSpan(0));
    }

    [Fact]
    public void ACellThatIsNotAStringIsNotAnInterval()
    {
        using CamusDataReader reader = ReaderFor("""[[42]]""", ColumnType.Integer64);

        Assert.True(reader.Read());
        InvalidCastException ex = Assert.Throws<InvalidCastException>(() => reader.GetTimeSpan(0));
        Assert.Contains("Integer64", ex.Message);
    }

    // ─── Dapper ───────────────────────────────────────────────────────────────

    [Fact]
    public void DapperHandlerBindsAndReadsACamusInterval()
    {
        CamusIntervalTypeHandler handler = new();
        CamusParameter parameter = new() { ParameterName = "@p" };

        handler.SetValue(parameter, new CamusInterval(1, 2, 0));

        Assert.Equal(ColumnType.String, parameter.ColumnType);
        Assert.Equal("P1M2D", parameter.Value);
        Assert.Equal(new CamusInterval(0, 1, 0), handler.Parse("P1D"));
        Assert.Throws<DataException>(() => handler.Parse("1 day"));
        Assert.Throws<DataException>(() => handler.Parse(42L));
    }

    [Fact]
    public void DapperHandlerBindsAndReadsATimeSpan()
    {
        CamusTimeSpanTypeHandler handler = new();
        CamusParameter parameter = new() { ParameterName = "@p" };

        handler.SetValue(parameter, TimeSpan.FromMinutes(90));

        Assert.Equal(ColumnType.String, parameter.ColumnType);
        Assert.Equal(TimeSpan.FromMinutes(90), parameter.Value);
        Assert.Equal(-TimeSpan.FromHours(36), handler.Parse("P-1DT-12H"));
        Assert.Equal(new TimeSpan(1, 2, 3, 4), handler.Parse("1.02:03:04"));
        Assert.Throws<DataException>(() => handler.Parse("P1M"));
        Assert.Throws<DataException>(() => handler.Parse("soon"));
        Assert.Throws<DataException>(() => handler.Parse(42L));
    }
}
