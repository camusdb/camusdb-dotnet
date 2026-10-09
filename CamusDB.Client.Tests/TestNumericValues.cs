/**
 * This file is part of CamusDB
 *
 * Offline coverage for the NUMERIC type (ColumnType.Numeric): the decimal conversions in CamusNumeric,
 * parameter binding, the REST row decoders, the gRPC codec, the data reader and the EF Core mapping.
 * NUMERIC travels as decimal text, so every test here checks that no digit goes through a double. No
 * server is required.
 */

using System.Data;
using System.Text.Json;
using CamusDB.Client.Transport;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Grpc = CamusDB.Grpc;

namespace CamusDB.Client.Tests;

public class TestNumericValues
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    // 29 significant digits: wider than a double, and still exact in a decimal.
    private const string DecimalWide = "12345678901234567890.123456789";

    // 38 significant digits: the NUMERIC maximum, which a decimal cannot hold.
    private const string NumericMax = "99999999999999999999999999999.999999999";

    // ─── CamusNumeric ─────────────────────────────────────────────────────────

    [Theory]
    [InlineData("0", "0")]
    [InlineData("1.5", "1.5")]
    [InlineData("-0.000000001", "-0.000000001")]
    [InlineData("1200", "1200")]
    [InlineData("+1.500", "1.500")]
    [InlineData(DecimalWide, DecimalWide)]
    public void TryToDecimalReadsExactValues(string text, string expected)
    {
        Assert.True(CamusNumeric.TryToDecimal(text, out decimal value));
        Assert.Equal(decimal.Parse(expected, System.Globalization.CultureInfo.InvariantCulture), value);
    }

    [Theory]
    [InlineData(NumericMax)]
    [InlineData("1234567890123456789012345678.123456789")]
    [InlineData("abc")]
    [InlineData("")]
    [InlineData(null)]
    public void TryToDecimalRefusesWhatADecimalWouldRound(string? text)
    {
        Assert.False(CamusNumeric.TryToDecimal(text, out _));
    }

    [Fact]
    public void ToDecimalThrowsInvalidCastForAWideValue()
    {
        InvalidCastException ex = Assert.Throws<InvalidCastException>(() => CamusNumeric.ToDecimal(NumericMax));
        Assert.Contains("GetString", ex.Message);
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
    public void UntypedDecimalBindsAsNumericWithEveryDigit()
    {
        ColumnValue value = Bind(decimal.Parse(DecimalWide, System.Globalization.CultureInfo.InvariantCulture), ColumnType.Null);

        Assert.Equal(ColumnType.Numeric, value.Type);
        Assert.Equal(DecimalWide, value.StrValue);
    }

    [Fact]
    public void DecimalKeepsItsScale()
    {
        ColumnValue value = Bind(12.50m, ColumnType.Null);

        Assert.Equal(ColumnType.Numeric, value.Type);
        Assert.Equal("12.50", value.StrValue);
    }

    [Fact]
    public void DbTypeDecimalBindsAsNumeric()
    {
        CamusParameter parameter = new() { DbType = DbType.Decimal };
        Assert.Equal(ColumnType.Numeric, parameter.ColumnType);

        ColumnValue value = BindByDbType(-0.1m, DbType.Decimal);
        Assert.Equal(ColumnType.Numeric, value.Type);
        Assert.Equal("-0.1", value.StrValue);

        Assert.Equal(DbType.Decimal, new CamusParameter("@q", ColumnType.Numeric, 1m).DbType);
    }

    [Theory]
    [InlineData(NumericMax, NumericMax)]
    [InlineData(42L, "42")]
    [InlineData(-7, "-7")]
    [InlineData(0.1, "0.1")]
    [InlineData(1e20, "1E+20")]
    public void NumericParameterConvertsOtherValuesToText(object value, string expected)
    {
        ColumnValue bound = Bind(value, ColumnType.Numeric);

        Assert.Equal(ColumnType.Numeric, bound.Type);
        Assert.Equal(expected, bound.StrValue);
    }

    [Theory]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    public void NumericParameterRefusesANonFiniteDouble(double value)
    {
        CamusException ex = Assert.Throws<CamusException>(() => Bind(value, ColumnType.Numeric));
        Assert.Equal("CADB0400", ex.Code);
    }

    [Fact]
    public void NullDecimalBindsAsNull()
    {
        Assert.Equal(ColumnType.Null, Bind(null, ColumnType.Numeric).Type);
        Assert.Equal(ColumnType.Null, Bind(DBNull.Value, ColumnType.Null).Type);
    }

    [Fact]
    public void DecimalArrayKeepsFloat64Elements()
    {
        // The server has no ARRAY(NUMERIC), so an array of decimals binds as ARRAY(FLOAT64) as before.
        ColumnValue array = Bind(new List<decimal> { 1.5m, 2m }, ColumnType.Array);

        Assert.Equal(ColumnType.Float64, array.ArrayElementType);
        Assert.Equal(new[] { 1.5, 2.0 }, array.ArrayValues!.Select(e => e.FloatValue));
        Assert.Equal(ColumnType.Float64, CamusDB.Dapper.CamusValue.Array(new[] { 1m }).ArrayElementType);
    }

    [Fact]
    public void DapperNumericValueBindsAsNumeric()
    {
        Assert.Equal(ColumnType.Numeric, CamusDB.Dapper.CamusValue.Numeric(1.5m).Type);
        Assert.Equal(ColumnType.Numeric, CamusDB.Dapper.CamusValue.Numeric(NumericMax).Type);
    }

    // ─── REST row decoding ────────────────────────────────────────────────────

    private static CamusDataReader ReaderFor(string rows, ColumnType declared = ColumnType.Numeric)
    {
        List<CamusColumnSchema> columns = [new() { Name = "p", Type = declared }];

        using JsonDocument doc = JsonDocument.Parse(rows);
        return new CamusDataReader(CamusResultSet.FromWire(columns, doc.RootElement.Clone()));
    }

    [Fact]
    public void NumericCellReadsAsADecimal()
    {
        using CamusDataReader reader = ReaderFor($"""[["{DecimalWide}"], ["-0.5"], [null]]""");

        Assert.Equal(typeof(decimal), reader.GetFieldType(0));

        Assert.True(reader.Read());
        decimal expected = decimal.Parse(DecimalWide, System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(ColumnType.Numeric, reader.GetColumnValue(0).Type);
        Assert.Equal(expected, reader.GetDecimal(0));
        Assert.Equal(expected, reader.GetValue(0));
        Assert.Equal(expected, reader.GetFieldValue<decimal>(0));
        Assert.Equal(DecimalWide, reader.GetString(0));

        Assert.True(reader.Read());
        Assert.Equal(-0.5m, reader.GetDecimal(0));
        Assert.Equal(-0.5, reader.GetDouble(0));
        Assert.Equal(-0.5, reader.GetFieldValue<double>(0));
        Assert.Equal(-0.5f, reader.GetFloat(0));

        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(0));
    }

    [Fact]
    public void WideNumericCellKeepsItsExactText()
    {
        using CamusDataReader reader = ReaderFor($"""[["{NumericMax}"]]""");
        Assert.True(reader.Read());

        Assert.Equal(NumericMax, reader.GetString(0));
        Assert.Throws<InvalidCastException>(() => reader.GetDecimal(0));
        Assert.Throws<InvalidCastException>(() => reader.GetValue(0));
        Assert.Equal(1e29, reader.GetDouble(0));
    }

    [Fact]
    public void NumberInANumericColumnBecomesANumeric()
    {
        // For a NULL p, SELECT COALESCE(p, 0) sends the int64 0 while the metadata says numeric.
        using CamusDataReader reader = ReaderFor("""[[0], [9007199254740993]]""");

        Assert.True(reader.Read());
        Assert.Equal(ColumnType.Numeric, reader.GetColumnValue(0).Type);
        Assert.Equal(0m, reader.GetDecimal(0));

        Assert.True(reader.Read());
        Assert.Equal(9007199254740993m, reader.GetDecimal(0));
        Assert.Equal(9007199254740993L, reader.GetInt64(0));
        Assert.Equal(9007199254740993L, reader.GetFieldValue<long>(0));
    }

    [Theory]
    [InlineData("""["1.5"]""")]
    [InlineData("""[3]""")]
    [InlineData("""[-12.25]""")]
    [InlineData("""[null]""")]
    [InlineData("""[true]""")]
    [InlineData("""[[1, 2]]""")]
    public void StreamingDecoderAgreesWithTheDomDecoder(string row)
    {
        ColumnType[] types = [ColumnType.Numeric];

        using JsonDocument doc = JsonDocument.Parse("[" + row + "]");
        CamusResultSet dom = CamusResultSet.FromWire([new() { Name = "p", Type = ColumnType.Numeric }], doc.RootElement.Clone());

        ColumnValue[] cells = new ColumnValue[1];
        Assert.True(CamusResultSet.TryDecodeRowInto(System.Text.Encoding.UTF8.GetBytes(row), types, cells));

        ColumnValue expected = dom.GetCell(0, 0);
        Assert.Equal(expected.Type, cells[0].Type);
        Assert.Equal(expected.StrValue, cells[0].StrValue);
        Assert.Equal(expected.BoolValue, cells[0].BoolValue);
    }

    // ─── gRPC codec ───────────────────────────────────────────────────────────

    [Fact]
    public void GrpcEncodesNumericAsText()
    {
        Grpc.Value encoded = GrpcValueCodec.Encode(new ColumnValue { Type = ColumnType.Numeric, StrValue = NumericMax });

        Assert.Equal(Grpc.Value.KindOneofCase.NumericValue, encoded.KindCase);
        Assert.Equal(NumericMax, encoded.NumericValue);

        ColumnValue decoded = GrpcValueCodec.Decode(encoded);
        Assert.Equal(ColumnType.Numeric, decoded.Type);
        Assert.Equal(NumericMax, decoded.StrValue);
    }

    [Fact]
    public void GrpcColumnTypeMatchesTheClientEnum()
    {
        Assert.Equal(Grpc.ColumnType.Numeric, GrpcValueCodec.ToGrpcColumnType(ColumnType.Numeric));
        Assert.Equal(ColumnType.Numeric, GrpcValueCodec.ToClientColumnType(Grpc.ColumnType.Numeric));
    }

    // ─── EF Core ──────────────────────────────────────────────────────────────

    [Fact]
    public void DecimalPropertyMapsToNumeric()
    {
        using NumericContext ctx = new(new DbContextOptionsBuilder<NumericContext>().UseCamusDB(ConnString).Options);
        IEntityType entityType = Assert.Single(ctx.GetService<IDesignTimeModel>().Model.GetEntityTypes());

        Assert.Equal("numeric", entityType.FindProperty(nameof(Price.Amount))!.GetColumnType());

        // HasPrecision cannot change the column: the server has no NUMERIC(P, S).
        Assert.Equal("numeric", entityType.FindProperty(nameof(Price.Tax))!.GetColumnType());

        string ddl = CamusDatabaseCreator.BuildCreateTableSql(entityType, "ef_prices");
        Assert.Contains("`Amount` NUMERIC NOT NULL", ddl);
        Assert.Contains("`Tax` NUMERIC NOT NULL", ddl);
        Assert.Contains("`Discount` NUMERIC NOT NULL DEFAULT (NUMERIC '0.25')", ddl);
        Assert.Contains("`Optional` NUMERIC", ddl);
    }

    [Fact]
    public void DecimalLiteralIsTheTypedNumericLiteral()
    {
        using NumericContext ctx = new(new DbContextOptionsBuilder<NumericContext>().UseCamusDB(ConnString).Options);
        RelationalTypeMapping mapping = ctx.GetService<IRelationalTypeMappingSource>().FindMapping(typeof(decimal))!;

        Assert.Equal("numeric", mapping.StoreType);
        Assert.Equal("NUMERIC '12345678901234567890.123456789'",
            mapping.GenerateSqlLiteral(decimal.Parse(DecimalWide, System.Globalization.CultureInfo.InvariantCulture)));
        Assert.Equal("NUMERIC '-0.5'", mapping.GenerateSqlLiteral(-0.5m));

        Assert.Equal("numeric", ctx.GetService<IRelationalTypeMappingSource>().FindMapping("decimal")!.StoreType);
    }

    [Fact]
    public void QueryWithADecimalConstantUsesTheTypedLiteral()
    {
        using NumericContext ctx = new(new DbContextOptionsBuilder<NumericContext>().UseCamusDB(ConnString).Options);

        string sql = ctx.Prices.Where(p => p.Amount > 9.99m).ToQueryString();

        Assert.Contains("NUMERIC '9.99'", sql);
    }

    public class Price
    {
        public string Id { get; set; } = "";

        public decimal Amount { get; set; }

        public decimal Tax { get; set; }

        public decimal Discount { get; set; }

        public decimal? Optional { get; set; }
    }

    private class NumericContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Price> Prices => Set<Price>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Price>(b =>
            {
                b.ToTable("ef_prices");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Tax).HasPrecision(18, 2);
                b.Property(e => e.Discount).HasDefaultValue(0.25m);
            });
        }
    }
}
