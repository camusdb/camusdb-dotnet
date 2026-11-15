/**
 * This file is part of CamusDB
 *
 * Coverage for the Math translations of the EF Core provider (CamusMathTranslator): the SQL text, the
 * overloads that do not translate on purpose, and a live run against a server on :5095 that has the
 * trigonometric functions (server commit 273183c5).
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkMath
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static MathContext NewContext() =>
        new(new DbContextOptionsBuilder<MathContext>().UseCamusDB(ConnString).Options);

    [Theory]
    [InlineData("sin", "sin(`e`.`angle`)")]
    [InlineData("cos", "cos(`e`.`angle`)")]
    [InlineData("tan", "tan(`e`.`angle`)")]
    [InlineData("asin", "asin(`e`.`ratio`)")]
    [InlineData("acos", "acos(`e`.`ratio`)")]
    [InlineData("atan", "atan(`e`.`ratio`)")]
    [InlineData("atan2", "atan2(`e`.`ratio`, `e`.`angle`)")]
    [InlineData("radians", "radians(`e`.`angle`)")]
    [InlineData("degrees", "degrees(`e`.`angle`)")]
    [InlineData("sqrt", "sqrt(`e`.`angle`)")]
    [InlineData("exp", "exp(`e`.`ratio`)")]
    [InlineData("ln", "ln(`e`.`angle`)")]
    [InlineData("log", "log(2.0, `e`.`angle`)")]
    [InlineData("log10", "log10(`e`.`angle`)")]
    [InlineData("pow", "pow(`e`.`angle`, 2.0)")]
    [InlineData("abs", "abs(`e`.`ratio`)")]
    [InlineData("ceil", "ceil(`e`.`ratio`)")]
    [InlineData("floor", "floor(`e`.`ratio`)")]
    [InlineData("trunc", "trunc(`e`.`ratio`)")]
    [InlineData("round", "round(`e`.`ratio`)")]
    [InlineData("round2", "round(`e`.`ratio`, 2)")]
    public void DoubleMethodsTranslateToServerFunctions(string function, string expected)
    {
        using MathContext ctx = NewContext();

        IQueryable<double> query = function switch
        {
            "sin" => ctx.Values.Select(m => Math.Sin(m.Angle)),
            "cos" => ctx.Values.Select(m => Math.Cos(m.Angle)),
            "tan" => ctx.Values.Select(m => Math.Tan(m.Angle)),
            "asin" => ctx.Values.Select(m => Math.Asin(m.Ratio)),
            "acos" => ctx.Values.Select(m => Math.Acos(m.Ratio)),
            "atan" => ctx.Values.Select(m => Math.Atan(m.Ratio)),
            "atan2" => ctx.Values.Select(m => Math.Atan2(m.Ratio, m.Angle)),
            "radians" => ctx.Values.Select(m => double.DegreesToRadians(m.Angle)),
            "degrees" => ctx.Values.Select(m => double.RadiansToDegrees(m.Angle)),
            "sqrt" => ctx.Values.Select(m => Math.Sqrt(m.Angle)),
            "exp" => ctx.Values.Select(m => Math.Exp(m.Ratio)),
            "ln" => ctx.Values.Select(m => Math.Log(m.Angle)),
            "log" => ctx.Values.Select(m => Math.Log(m.Angle, 2.0)),
            "log10" => ctx.Values.Select(m => Math.Log10(m.Angle)),
            "pow" => ctx.Values.Select(m => Math.Pow(m.Angle, 2.0)),
            "abs" => ctx.Values.Select(m => Math.Abs(m.Ratio)),
            "ceil" => ctx.Values.Select(m => Math.Ceiling(m.Ratio)),
            "floor" => ctx.Values.Select(m => Math.Floor(m.Ratio)),
            "trunc" => ctx.Values.Select(m => Math.Truncate(m.Ratio)),
            "round" => ctx.Values.Select(m => Math.Round(m.Ratio, MidpointRounding.AwayFromZero)),
            _ => ctx.Values.Select(m => Math.Round(m.Ratio, 2, MidpointRounding.AwayFromZero)),
        };

        Assert.Contains($"SELECT {expected}", query.ToQueryString(), StringComparison.Ordinal);
    }

    [Fact]
    public void IntegerAndDecimalMethodsKeepTheirType()
    {
        using MathContext ctx = NewContext();

        Assert.Contains("SELECT abs(`e`.`count`)", ctx.Values.Select(m => Math.Abs(m.Count)).ToQueryString(), StringComparison.Ordinal);
        Assert.Contains("SELECT sign(`e`.`count`)", ctx.Values.Select(m => Math.Sign(m.Count)).ToQueryString(), StringComparison.Ordinal);
        Assert.Contains("SELECT floor(`e`.`price`)", ctx.Values.Select(m => Math.Floor(m.Price)).ToQueryString(), StringComparison.Ordinal);
    }

    [Fact]
    public void MathInAFilterTranslates()
    {
        using MathContext ctx = NewContext();

        string sql = ctx.Values.Where(m => Math.Sin(m.Angle) > 0.5).ToQueryString();

        Assert.Contains("WHERE sin(`e`.`angle`) > 0.5", sql, StringComparison.Ordinal);
    }

    // The server rounds a midpoint away from zero; Math.Round without a mode rounds it to even.
    [Fact]
    public void RoundToEvenDoesNotTranslate()
    {
        using MathContext ctx = NewContext();

        Assert.Throws<InvalidOperationException>(() => ctx.Values.Where(m => Math.Round(m.Ratio) > 1).ToQueryString());
        Assert.Throws<InvalidOperationException>(() => ctx.Values.Where(m => Math.Round(m.Ratio, 1) > 1).ToQueryString());
    }

    // sign of a numeric is a numeric on the server, not an int.
    [Fact]
    public void SignOfADecimalDoesNotTranslate()
    {
        using MathContext ctx = NewContext();

        Assert.Throws<InvalidOperationException>(() => ctx.Values.Where(m => Math.Sign(m.Price) > 0).ToQueryString());
    }

    [Fact]
    public async Task MathRunsOnTheServer()
    {
        await using MathContext ctx = NewContext();
        await ctx.Database.EnsureCreatedAsync();

        string tag = Guid.NewGuid().ToString("n");
        ctx.Values.Add(new MathValue { Id = Guid.NewGuid().ToString("n"), Tag = tag, Angle = 90, Ratio = -2.5, Count = -3, Price = 12.75m });
        await ctx.SaveChangesAsync();

        var row = await ctx.Values.AsNoTracking().Where(m => m.Tag == tag).Select(m => new
        {
            Sin = Math.Sin(double.DegreesToRadians(m.Angle)),
            Cos = Math.Cos(0 * m.Angle),
            Atan2 = Math.Atan2(1.0, 1.0 + 0 * m.Angle),
            Degrees = double.RadiansToDegrees(Math.Acos(-1 + 0 * m.Angle)),
            Abs = Math.Abs(m.Ratio),
            AbsCount = Math.Abs(m.Count),
            Sign = Math.Sign(m.Count),
            Round = Math.Round(m.Ratio, MidpointRounding.AwayFromZero),
            Floor = Math.Floor(m.Price),
            Ln = Math.Log(Math.Exp(2 + 0 * m.Angle)),
            Log2 = Math.Log(8 + 0 * m.Angle, 2),
            Pow = Math.Pow(m.Angle, 0.5) * Math.Pow(m.Angle, 0.5),
        }).SingleAsync();

        Assert.Equal(1.0, row.Sin, 12);
        Assert.Equal(1.0, row.Cos, 12);
        Assert.Equal(Math.PI / 4, row.Atan2, 12);
        Assert.Equal(180.0, row.Degrees, 9);
        Assert.Equal(2.5, row.Abs);
        Assert.Equal(3, row.AbsCount);
        Assert.Equal(-1, row.Sign);
        Assert.Equal(-3.0, row.Round);
        Assert.Equal(12m, row.Floor);
        Assert.Equal(2.0, row.Ln, 12);
        Assert.Equal(3.0, row.Log2, 12);
        Assert.Equal(90.0, row.Pow, 9);

        Assert.Equal(1, await ctx.Values.CountAsync(m => m.Tag == tag && Math.Sin(double.DegreesToRadians(m.Angle)) > 0.99));
    }

    private sealed class MathContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<MathValue> Values => Set<MathValue>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<MathValue>(b =>
            {
                b.ToTable("ef_math_values_v1");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnName("id");
                b.Property(e => e.Tag).HasColumnName("tag");
                b.Property(e => e.Angle).HasColumnName("angle");
                b.Property(e => e.Ratio).HasColumnName("ratio");
                b.Property(e => e.Count).HasColumnName("count");
                b.Property(e => e.Price).HasColumnName("price");
            });
        }
    }

    private sealed class MathValue
    {
        public string Id { get; set; } = "";
        public string Tag { get; set; } = "";
        public double Angle { get; set; }
        public double Ratio { get; set; }
        public int Count { get; set; }
        public decimal Price { get; set; }
    }
}
