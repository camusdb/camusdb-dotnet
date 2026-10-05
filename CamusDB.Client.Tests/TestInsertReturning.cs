/**
 * This file is part of CamusDB
 *
 * Offline coverage for INSERT … RETURNING: the keyword scan, the REST request and response shapes of
 * the ADO.NET command, and the EF Core read-back of store-generated columns. The HTTP calls are faked
 * with Flurl's HttpTest, so no server is needed. TestInsertReturningLive covers a real server.
 */

using System.Text.Json;
using CamusDB.EntityFrameworkCore;
using Flurl.Http.Testing;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestInsertReturning
{
    // ─── Keyword scan ─────────────────────────────────────────────────────────

    [Theory]
    [InlineData("INSERT INTO t (a) VALUES (1) RETURNING a")]
    [InlineData("insert into t (a) values (1) returning *")]
    [InlineData("INSERT INTO t (a) VALUES (1)\nRETURNING\ta")]
    [InlineData("INSERT INTO t (a) VALUES ('x') RETURNING a")]
    [InlineData("INSERT INTO t (a) VALUES ('it''s') RETURNING a")]
    [InlineData("INSERT INTO t (a) VALUES ('a\\'b') RETURNING a")]
    [InlineData("INSERT INTO t SELECT * FROM s /* comment */ RETURNING id")]
    public void KeywordIsFound(string sql)
        => Assert.True(CamusSqlSyntax.HasReturningKeyword(sql));

    [Theory]
    [InlineData("INSERT INTO t (a) VALUES (1)")]
    [InlineData("INSERT INTO t (a) VALUES ('RETURNING')")]
    [InlineData("INSERT INTO t (a) VALUES ('it''s RETURNING')")]
    [InlineData("INSERT INTO t (`returning`) VALUES (1)")]
    [InlineData("INSERT INTO t (a) VALUES (@returning)")]
    [InlineData("INSERT INTO t (a) VALUES (1) -- RETURNING a")]
    [InlineData("INSERT INTO t (a) VALUES (1) /* RETURNING a */")]
    [InlineData("INSERT INTO returning_log (a) VALUES (1)")]
    [InlineData("INSERT INTO t (a) VALUES (\"RETURNING\")")]
    [InlineData("INSERT INTO t (a) VALUES ('unterminated RETURNING")]
    public void KeywordIsNotFound(string sql)
        => Assert.False(CamusSqlSyntax.HasReturningKeyword(sql));

    // ─── ADO.NET over REST ────────────────────────────────────────────────────

    private const string ReturningResponse = """
        {"status":"ok","rows":2,
         "columns":[{"name":"id","type":1},{"name":"n","type":2}],
         "returningRows":[["6849f3aa",10],["6849f3bb",20]]}
        """;

    /// <summary>The JSON body of the one call that the test made. The driver sends its own
    /// <c>HttpContent</c>, which <c>HttpTest</c> does not capture as text, so the test reads it.</summary>
    private static Task<string> RequestBodyAsync(HttpTest httpTest)
        => Assert.Single(httpTest.CallLog).HttpRequestMessage.Content!.ReadAsStringAsync();

    private static CamusConnection Connection(int port)
        => new(new CamusConnectionStringBuilder($"Endpoint=http://localhost:{port};Database=test"));

    [Fact]
    public async Task ReaderReturnsTheReturningRows()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9301/execute-sql-non-query").RespondWith(ReturningResponse);

        using CamusConnection connection = Connection(9301);
        await using CamusCommand command = connection.CreateCamusCommand(
            "INSERT INTO t (id, n) VALUES (GEN_ID(), 10), (GEN_ID(), 20) RETURNING id, n");
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.Equal(2, reader.RecordsAffected);
        Assert.Equal(2, reader.FieldCount);
        Assert.Equal("id", reader.GetName(0));
        Assert.Equal("n", reader.GetName(1));
        Assert.True(reader.HasRows);

        Assert.True(await reader.ReadAsync());
        Assert.Equal("6849f3aa", reader.GetString(0));
        Assert.Equal(10L, reader.GetInt64(1));
        Assert.True(await reader.ReadAsync());
        Assert.Equal("6849f3bb", reader.GetString(0));
        Assert.Equal(20L, reader.GetInt64(1));
        Assert.False(await reader.ReadAsync());

        // The reader asks for the rows, so the request does not carry the count-only flag.
        Assert.DoesNotContain("discardReturningRows", await RequestBodyAsync(httpTest));
    }

    [Fact]
    public async Task ScalarReturnsTheFirstReturnedValue()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9302/execute-sql-non-query").RespondWith(ReturningResponse);

        using CamusConnection connection = Connection(9302);
        await using CamusCommand command = connection.CreateCamusCommand("INSERT INTO t (n) VALUES (10) RETURNING id");

        Assert.Equal("6849f3aa", await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ReturningThatInsertsNoRowsKeepsTheSchema()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9303/execute-sql-non-query").RespondWith(
            """{"status":"ok","rows":0,"columns":[{"name":"id","type":1}],"returningRows":[]}""");

        using CamusConnection connection = Connection(9303);
        await using CamusCommand command = connection.CreateCamusCommand("INSERT INTO t SELECT * FROM s WHERE FALSE RETURNING id");
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.Equal(0, reader.RecordsAffected);
        Assert.Equal(1, reader.FieldCount);
        Assert.False(reader.HasRows);
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task ReaderWithoutReturningReportsTheCountOnly()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9304/execute-sql-non-query").RespondWith("""{"status":"ok","rows":3}""");

        using CamusConnection connection = Connection(9304);
        await using CamusCommand command = connection.CreateCamusCommand("UPDATE t SET n = 1 WHERE TRUE");
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.Equal(3, reader.RecordsAffected);
        Assert.Equal(0, reader.FieldCount);
        Assert.False(await reader.ReadAsync());
    }

    [Fact]
    public async Task NonQueryAsksForTheCountOnly()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9305/execute-sql-non-query").RespondWith("""{"status":"ok","rows":2}""");

        using CamusConnection connection = Connection(9305);
        await using CamusCommand command = connection.CreateCamusCommand(
            "INSERT INTO t (n) VALUES (1), (2) RETURNING id");

        Assert.Equal(2, await command.ExecuteNonQueryAsync());

        using JsonDocument body = JsonDocument.Parse(await RequestBodyAsync(httpTest));
        Assert.True(body.RootElement.GetProperty("discardReturningRows").GetBoolean());
    }

    [Fact]
    public void CountOnlyFlagIsOmittedWhenFalse()
    {
        // A false flag stays out of the JSON, so the request shape that a server without RETURNING
        // accepts does not change.
        string json = JsonSerializer.Serialize(
            new CamusExecuteSqlNonQueryRequest { Sql = "UPDATE t SET a = 1" },
            CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryRequest);

        Assert.DoesNotContain("discardReturningRows", json);
    }

    [Fact]
    public async Task StreamReaderSendsInsertReturningToTheQueryStream()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9306/execute-sql-query-stream").RespondWith(
            """{"status":"ok","columns":[{"name":"id","type":1}]}""" + "\n" +
            """["6849f3aa"]""" + "\n" +
            """{"status":"ok","total":1,"serverTimeMs":0.5}""" + "\n");

        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:9306;Database=test;IsolationLevel=Serializable;Locking=Optimistic");
        using CamusConnection connection = new(builder);
        await using CamusCommand command = connection.CreateCamusCommand("INSERT INTO t (n) VALUES (1) RETURNING id");
        await using CamusDataReader reader = await command.ExecuteStreamReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("6849f3aa", reader.GetString(0));
        Assert.False(await reader.ReadAsync());

        // The autocommit INSERT runs in a writable transaction, so its options travel with it. The
        // query endpoint refuses the count-only flag, so the request must not carry it.
        using JsonDocument body = JsonDocument.Parse(await RequestBodyAsync(httpTest));
        Assert.Equal("Serializable", body.RootElement.GetProperty("isolationLevel").GetString());
        Assert.Equal("Optimistic", body.RootElement.GetProperty("locking").GetString());
        Assert.False(body.RootElement.TryGetProperty("discardReturningRows", out _));
    }

    [Fact]
    public async Task StreamReaderKeepsPlainInsertOnTheNonQueryEndpoint()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9307/execute-sql-non-query").RespondWith("""{"status":"ok","rows":1}""");

        using CamusConnection connection = Connection(9307);
        await using CamusCommand command = connection.CreateCamusCommand("INSERT INTO t (`returning`) VALUES (1)");
        await using CamusDataReader reader = await command.ExecuteStreamReaderAsync();

        Assert.Equal(1, reader.RecordsAffected);
        httpTest.ShouldHaveCalled("http://localhost:9307/execute-sql-non-query").Times(1);
    }

    // ─── EF Core ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task SaveChangesReadsStoreGeneratedColumnsBack()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9308/execute-sql-non-query").RespondWith(
            """{"status":"ok","rows":1,"columns":[{"name":"Serial","type":2},{"name":"Status","type":3}],"returningRows":[[41,"new"]]}""");

        DbContextOptions<GadgetContext> options = new DbContextOptionsBuilder<GadgetContext>()
            .UseCamusDB("Endpoint=http://localhost:9308;Database=test")
            .Options;

        Gadget gadget = new() { Id = "6849f3aa6849f3aa6849f3aa", Name = "g" };
        await using (GadgetContext ctx = new(options))
        {
            ctx.Gadgets.Add(gadget);
            await ctx.SaveChangesAsync();
        }

        Assert.Equal(41L, gadget.Serial);
        Assert.Equal("new", gadget.Status);

        using JsonDocument body = JsonDocument.Parse(await RequestBodyAsync(httpTest));
        string sql = body.RootElement.GetProperty("sql").GetString()!;
        Assert.Equal("INSERT INTO `gadgets` (`Id`, `Name`) VALUES (@p0, @p1) RETURNING `Serial`, `Status`", sql.Trim());
        Assert.False(body.RootElement.TryGetProperty("discardReturningRows", out _));
    }

    [Fact]
    public async Task SaveChangesWritesAnExplicitValueAndReadsNothingBack()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9309/execute-sql-non-query").RespondWith("""{"status":"ok","rows":1}""");

        DbContextOptions<GadgetContext> options = new DbContextOptionsBuilder<GadgetContext>()
            .UseCamusDB("Endpoint=http://localhost:9309;Database=test")
            .Options;

        await using (GadgetContext ctx = new(options))
        {
            ctx.Gadgets.Add(new Gadget { Id = "6849f3aa6849f3aa6849f3aa", Name = "g", Serial = 7, Status = "old" });
            await ctx.SaveChangesAsync();
        }

        using JsonDocument body = JsonDocument.Parse(await RequestBodyAsync(httpTest));
        string sql = body.RootElement.GetProperty("sql").GetString()!;
        Assert.DoesNotContain("RETURNING", sql);
    }

    [Fact]
    public async Task UpdateReadsNothingBack()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9310/execute-sql-non-query").RespondWith("""{"status":"ok","rows":1}""");

        DbContextOptions<GadgetContext> options = new DbContextOptionsBuilder<GadgetContext>()
            .UseCamusDB("Endpoint=http://localhost:9310;Database=test")
            .Options;

        await using (GadgetContext ctx = new(options))
        {
            Gadget gadget = new() { Id = "6849f3aa6849f3aa6849f3aa", Name = "g", Serial = 7, Status = "old" };
            ctx.Gadgets.Attach(gadget);
            gadget.Name = "h";
            await ctx.SaveChangesAsync();
        }

        using JsonDocument body = JsonDocument.Parse(await RequestBodyAsync(httpTest));
        Assert.StartsWith("UPDATE", body.RootElement.GetProperty("sql").GetString()!.Trim());
        Assert.DoesNotContain("RETURNING", body.RootElement.GetProperty("sql").GetString());
    }

    public class Gadget
    {
        public string Id { get; set; } = "";

        public string Name { get; set; } = "";

        public long Serial { get; set; }

        public string? Status { get; set; }
    }

    private sealed class GadgetContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Gadget> Gadgets => Set<Gadget>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Gadget>(b =>
            {
                b.ToTable("gadgets");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedNever();
                b.Property(e => e.Serial).HasDefaultValueSql("nextval('gadget_serial')");
                b.Property(e => e.Status).HasDefaultValue("new");
            });
        }
    }
}
