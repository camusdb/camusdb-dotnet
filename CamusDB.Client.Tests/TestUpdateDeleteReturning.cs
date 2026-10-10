/**
 * This file is part of CamusDB
 *
 * Offline coverage for UPDATE … RETURNING and DELETE … RETURNING: the endpoint that each ADO.NET entry
 * point picks, and the EF Core read-back of store-generated columns on an update. The HTTP calls are
 * faked with Flurl's HttpTest, so no server is needed. TestUpdateDeleteReturningLive covers a real
 * server.
 */

using System.Text.Json;
using CamusDB.EntityFrameworkCore;
using Flurl.Http.Testing;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestUpdateDeleteReturning
{
    /// <summary>The JSON body of the one call that the test made.</summary>
    private static Task<string> RequestBodyAsync(HttpTest httpTest)
        => Assert.Single(httpTest.CallLog).HttpRequestMessage.Content!.ReadAsStringAsync();

    private static CamusConnection Connection(int port)
        => new(new CamusConnectionStringBuilder($"Endpoint=http://localhost:{port};Database=test"));

    // ─── ADO.NET over REST ────────────────────────────────────────────────────

    [Theory]
    [InlineData(9311, "UPDATE t SET n = n * 2 WHERE n > 5 RETURNING id, n")]
    [InlineData(9312, "DELETE FROM t WHERE n > 5 LIMIT 10 RETURNING id, n")]
    public async Task ReaderReturnsTheReturningRows(int port, string sql)
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo($"http://localhost:{port}/execute-sql-non-query").RespondWith("""
            {"status":"ok","rows":2,
             "columns":[{"name":"id","type":1},{"name":"n","type":2}],
             "returningRows":[["6849f3aa",20],["6849f3bb",40]]}
            """);

        using CamusConnection connection = Connection(port);
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.Equal(2, reader.RecordsAffected);
        Assert.True(await reader.ReadAsync());
        Assert.Equal("6849f3aa", reader.GetString(0));
        Assert.Equal(20L, reader.GetInt64(1));
        Assert.True(await reader.ReadAsync());
        Assert.Equal(40L, reader.GetInt64(1));
        Assert.False(await reader.ReadAsync());

        Assert.DoesNotContain("discardReturningRows", await RequestBodyAsync(httpTest));
    }

    [Theory]
    [InlineData(9313, "UPDATE t SET n = 1 WHERE n > 5 RETURNING id")]
    [InlineData(9313, "delete from t where n > 5 returning *")]
    public async Task NonQueryAsksForTheCountOnly(int port, string sql)
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo($"http://localhost:{port}/execute-sql-non-query").RespondWith("""{"status":"ok","rows":3}""");

        using CamusConnection connection = Connection(port);
        await using CamusCommand command = connection.CreateCamusCommand(sql);

        Assert.Equal(3, await command.ExecuteNonQueryAsync());

        using JsonDocument body = JsonDocument.Parse(await RequestBodyAsync(httpTest));
        Assert.True(body.RootElement.GetProperty("discardReturningRows").GetBoolean());
    }

    [Theory]
    [InlineData(9314, "UPDATE t SET n = 2 WHERE n = 1 RETURNING id")]
    [InlineData(9314, "DELETE FROM t WHERE n = 1 RETURNING id")]
    public async Task StreamReaderSendsTheStatementToTheQueryStream(int port, string sql)
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo($"http://localhost:{port}/execute-sql-query-stream").RespondWith(
            """{"status":"ok","columns":[{"name":"id","type":1}]}""" + "\n" +
            """["6849f3aa"]""" + "\n" +
            """{"status":"ok","total":1,"serverTimeMs":0.5}""" + "\n");

        CamusConnectionStringBuilder builder = new(
            $"Endpoint=http://localhost:{port};Database=test;IsolationLevel=Serializable;Locking=Optimistic");
        using CamusConnection connection = new(builder);
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        await using CamusDataReader reader = await command.ExecuteStreamReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("6849f3aa", reader.GetString(0));
        Assert.False(await reader.ReadAsync());

        // The autocommit write runs in a writable transaction, so its options travel with it. The query
        // endpoint refuses the count-only flag, so the request must not carry it.
        using JsonDocument body = JsonDocument.Parse(await RequestBodyAsync(httpTest));
        Assert.Equal("Serializable", body.RootElement.GetProperty("isolationLevel").GetString());
        Assert.Equal("Optimistic", body.RootElement.GetProperty("locking").GetString());
        Assert.False(body.RootElement.TryGetProperty("discardReturningRows", out _));
    }

    [Theory]
    [InlineData(9315, "UPDATE t SET `returning` = 1 WHERE TRUE")]
    [InlineData(9315, "DELETE FROM t WHERE name = 'RETURNING'")]
    public async Task StreamReaderKeepsAPlainWriteOnTheNonQueryEndpoint(int port, string sql)
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo($"http://localhost:{port}/execute-sql-non-query").RespondWith("""{"status":"ok","rows":4}""");

        using CamusConnection connection = Connection(port);
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        await using CamusDataReader reader = await command.ExecuteStreamReaderAsync();

        Assert.Equal(4, reader.RecordsAffected);
        httpTest.ShouldHaveCalled($"http://localhost:{port}/execute-sql-non-query").Times(1);
    }

    // ─── EF Core ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task UpdateReadsStoreGeneratedColumnsBack()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9316/execute-sql-non-query").RespondWith(
            """{"status":"ok","rows":1,"columns":[{"name":"Status","type":3}],"returningRows":[["changed"]]}""");

        DbContextOptions<GizmoContext> options = new DbContextOptionsBuilder<GizmoContext>()
            .UseCamusDB("Endpoint=http://localhost:9316;Database=test")
            .Options;

        Gizmo gizmo = new() { Id = "6849f3aa6849f3aa6849f3aa", Name = "g", Status = "new" };
        await using (GizmoContext ctx = new(options))
        {
            ctx.Gizmos.Attach(gizmo);
            gizmo.Name = "h";
            await ctx.SaveChangesAsync();
        }

        Assert.Equal("changed", gizmo.Status);

        using JsonDocument body = JsonDocument.Parse(await RequestBodyAsync(httpTest));
        string sql = body.RootElement.GetProperty("sql").GetString()!;
        Assert.Equal("UPDATE `gizmos` SET `Name` = @p0 WHERE `Id` = @p1 RETURNING `Status`", sql.Trim());
        Assert.False(body.RootElement.TryGetProperty("discardReturningRows", out _));
    }

    [Fact]
    public async Task UpdateThatReturnsNoRowIsAConcurrencyFailure()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9317/execute-sql-non-query").RespondWith(
            """{"status":"ok","rows":0,"columns":[{"name":"Status","type":3}],"returningRows":[]}""");

        DbContextOptions<GizmoContext> options = new DbContextOptionsBuilder<GizmoContext>()
            .UseCamusDB("Endpoint=http://localhost:9317;Database=test")
            .Options;

        await using GizmoContext ctx = new(options);
        Gizmo gizmo = new() { Id = "6849f3aa6849f3aa6849f3aa", Name = "g", Status = "new" };
        ctx.Gizmos.Attach(gizmo);
        gizmo.Name = "h";

        await Assert.ThrowsAsync<DbUpdateConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task DeleteReadsNothingBack()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9318/execute-sql-non-query").RespondWith("""{"status":"ok","rows":1}""");

        DbContextOptions<GizmoContext> options = new DbContextOptionsBuilder<GizmoContext>()
            .UseCamusDB("Endpoint=http://localhost:9318;Database=test")
            .Options;

        await using (GizmoContext ctx = new(options))
        {
            Gizmo gizmo = new() { Id = "6849f3aa6849f3aa6849f3aa", Name = "g", Status = "new" };
            ctx.Gizmos.Attach(gizmo);
            ctx.Gizmos.Remove(gizmo);
            await ctx.SaveChangesAsync();
        }

        using JsonDocument body = JsonDocument.Parse(await RequestBodyAsync(httpTest));
        string sql = body.RootElement.GetProperty("sql").GetString()!;
        Assert.Equal("DELETE FROM `gizmos` WHERE `Id` = @p0", sql.Trim());
    }

    public class Gizmo
    {
        public string Id { get; set; } = "";

        public string Name { get; set; } = "";

        public string? Status { get; set; }
    }

    private sealed class GizmoContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Gizmo> Gizmos => Set<Gizmo>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Gizmo>(b =>
            {
                b.ToTable("gizmos");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedNever();
                // Store-generated on add and on update, so an update does not write it and reads it back.
                b.Property(e => e.Status).HasDefaultValue("new").ValueGeneratedOnAddOrUpdate();
            });
        }
    }
}
