/**
 * This file is part of CamusDB
 *
 * Live coverage for the NUMERIC type over REST (buffered and streamed) and gRPC, and through EF Core.
 * Needs a server on :5095 (REST) and :5096 (gRPC) that supports NUMERIC.
 */

using System.Globalization;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestNumericLive : BaseTest
{
    private const string DecimalWide = "12345678901234567890.123456789";

    private const string NumericMax = "99999999999999999999999999999.999999999";

    private static CamusConnectionStringBuilder BuilderFor(string protocol) => protocol == "grpc"
        ? new("Endpoint=http://localhost:5096;Database=test;Protocol=grpc")
        : new("Endpoint=http://localhost:5095;Database=test");

    private static async Task<CamusConnection> OpenAsync(string protocol)
    {
        CamusConnection connection = new(BuilderFor(protocol));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync(ifNotExists: true);
        return connection;
    }

    private static async Task InsertAsync(CamusConnection connection, string table, string name, object? price)
    {
        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, price) VALUES (@id, @name, @price)");

        insert.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
        insert.Parameters.Add("@name", ColumnType.String, name);
        insert.Parameters.Add("@price", ColumnType.Null, price ?? DBNull.Value);

        Assert.Equal(1, await insert.ExecuteNonQueryAsync());
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task DecimalParametersRoundTripExactly(string protocol)
    {
        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateTempTableAsync(connection, "numeric",
            " id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, price NUMERIC");

        decimal wide = decimal.Parse(DecimalWide, CultureInfo.InvariantCulture);

        await InsertAsync(connection, table, "wide", wide);
        await InsertAsync(connection, table, "cents", 0.10m);
        await InsertAsync(connection, table, "negative", -0.000000001m);
        await InsertAsync(connection, table, "none", null);

        // A string parameter typed NUMERIC binds a value wider than a decimal with every digit.
        await using (CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, price) VALUES (@id, @name, @price)"))
        {
            insert.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
            insert.Parameters.Add("@name", ColumnType.String, "max");
            insert.Parameters.Add("@price", ColumnType.Numeric, NumericMax);
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        Dictionary<string, (bool IsNull, string Text)> rows = [];

        await using (CamusCommand select = connection.CreateCamusCommand($"SELECT name, price FROM {table}"))
        await using (CamusDataReader reader = await select.ExecuteReaderAsync())
        {
            Assert.Equal(typeof(decimal), reader.GetFieldType(1));

            while (await reader.ReadAsync())
            {
                string name = reader.GetString(0);
                rows[name] = (reader.IsDBNull(1), reader.IsDBNull(1) ? "" : reader.GetString(1));

                if (name == "wide")
                    Assert.Equal(wide, reader.GetDecimal(1));

                if (name == "max")
                    Assert.Throws<InvalidCastException>(() => reader.GetDecimal(1));
            }
        }

        Assert.Equal(DecimalWide, rows["wide"].Text);
        Assert.Equal("0.1", rows["cents"].Text);
        Assert.Equal("-0.000000001", rows["negative"].Text);
        Assert.True(rows["none"].IsNull);
        Assert.Equal(NumericMax, rows["max"].Text);

        // A decimal parameter compares exactly with the NUMERIC column.
        await using (CamusCommand filter = connection.CreateCamusCommand($"SELECT name FROM {table} WHERE price = @price"))
        {
            filter.Parameters.Add("@price", ColumnType.Null, wide);

            await using CamusDataReader reader = await filter.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal("wide", reader.GetString(0));
            Assert.False(await reader.ReadAsync());
        }

        // SUM of NUMERIC is an exact NUMERIC.
        await using (CamusCommand sum = connection.CreateCamusCommand(
            $"SELECT SUM(price) AS total FROM {table} WHERE name = 'cents' OR name = 'negative'"))
        {
            await using CamusDataReader reader = await sum.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(0.099999999m, reader.GetDecimal(0));
        }
    }

    [Fact]
    public async Task StreamedReaderDecodesNumeric()
    {
        await using CamusConnection connection = await OpenAsync("");
        string table = await CreateTempTableAsync(connection, "numeric_stream",
            " id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, price NUMERIC");

        await InsertAsync(connection, table, "wide", decimal.Parse(DecimalWide, CultureInfo.InvariantCulture));

        await using CamusCommand select = connection.CreateCamusCommand($"SELECT price FROM {table}");
        await using CamusDataReader reader = await select.ExecuteStreamReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal(ColumnType.Numeric, reader.GetColumnValue(0).Type);
        Assert.Equal(DecimalWide, reader.GetString(0));
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task OutOfRangeNumericIsRefused()
    {
        await using CamusConnection connection = await OpenAsync("");
        string table = await CreateTempTableAsync(connection, "numeric_range",
            " id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, price NUMERIC");

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, price) VALUES (@id, @name, @price)");

        insert.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
        insert.Parameters.Add("@name", ColumnType.String, "too big");
        insert.Parameters.Add("@price", ColumnType.Numeric, "1" + new string('0', 30));

        CamusException ex = await Assert.ThrowsAsync<CamusException>(() => insert.ExecuteNonQueryAsync());
        Assert.Equal("CADB0417", ex.Code);
    }

    [Fact]
    public async Task EntityFrameworkSavesAndFiltersDecimals()
    {
        DbContextOptions<PriceContext> options = new DbContextOptionsBuilder<PriceContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test").Options;

        await using (PriceContext ctx = new(options))
            await ctx.Database.EnsureCreatedAsync();

        string tag = Guid.NewGuid().ToString("n");
        decimal wide = decimal.Parse(DecimalWide, CultureInfo.InvariantCulture);

        await using (PriceContext ctx = new(options))
        {
            ctx.Prices.Add(new PriceRow { Id = CamusObjectIdGenerator.GenerateAsString(), Tag = tag, Amount = wide, Optional = null });
            ctx.Prices.Add(new PriceRow { Id = CamusObjectIdGenerator.GenerateAsString(), Tag = tag, Amount = 9.99m, Optional = 0.5m });
            ctx.Prices.Add(new PriceRow { Id = CamusObjectIdGenerator.GenerateAsString(), Tag = tag, Amount = 10.01m, Optional = -1m });
            await ctx.SaveChangesAsync();
        }

        await using (PriceContext ctx = new(options))
        {
            // A constant becomes the NUMERIC '…' literal; a captured variable becomes a NUMERIC parameter.
            decimal threshold = 10m;

            List<decimal> above = await ctx.Prices.AsNoTracking()
                .Where(p => p.Tag == tag && p.Amount > 9.99m)
                .OrderBy(p => p.Amount)
                .Select(p => p.Amount)
                .ToListAsync();

            Assert.Equal([10.01m, wide], above);

            List<decimal> belowThreshold = await ctx.Prices.AsNoTracking()
                .Where(p => p.Tag == tag && p.Amount < threshold)
                .Select(p => p.Amount)
                .ToListAsync();

            Assert.Equal([9.99m], belowThreshold);

            PriceRow exact = await ctx.Prices.AsNoTracking().SingleAsync(p => p.Tag == tag && p.Amount == wide);
            Assert.Null(exact.Optional);

            decimal total = await ctx.Prices.Where(p => p.Tag == tag && p.Optional != null).SumAsync(p => p.Optional!.Value);
            Assert.Equal(-0.5m, total);
        }
    }

    public class PriceRow
    {
        public string Id { get; set; } = "";

        public string Tag { get; set; } = "";

        public decimal Amount { get; set; }

        public decimal? Optional { get; set; }
    }

    private class PriceContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<PriceRow> Prices => Set<PriceRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<PriceRow>(b =>
            {
                b.ToTable("ef_numeric_prices");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Tag).HasMaxLength(32);
            });
        }
    }
}
