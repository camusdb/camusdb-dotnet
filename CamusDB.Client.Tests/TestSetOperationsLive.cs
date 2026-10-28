/**
 * This file is part of CamusDB
 *
 * Live coverage for UNION, INTERSECT and EXCEPT on both transports: SQL through ADO.NET (including a
 * statement that starts with a parenthesized operand, and a prepared one), and the LINQ set operators
 * through EF Core, whose generated SQL the server runs as it is. Requires a server with set operations,
 * so the tests are opt-in: set CAMUSDB_TEST_SET_OPERATIONS=true. The REST endpoint comes from
 * CAMUSDB_TEST_ENDPOINT (default localhost:5095), and the gRPC endpoint from CAMUSDB_TEST_GRPC_ENDPOINT
 * (default localhost:5096).
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestSetOperationsLive
{
    private static bool Enabled
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_SET_OPERATIONS"), "true", StringComparison.OrdinalIgnoreCase);

    private static string ConnString(string protocol) => protocol == "grpc"
        ? $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096"};Database=test;Protocol=grpc"
        : $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095"};Database=test";

    // One fixed table: EF Core builds one model for each context type, so the table name cannot change
    // from test to test. The rows are always the same, so a rerun or a second protocol rewrites them.
    private const string Table = "setop_items";

    /// <summary>Makes sure that the table holds exactly the rows a1..a5 (n = 1..5).</summary>
    private static async Task<string> SeedAsync(string protocol)
    {
        await using CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString(protocol)));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync("test", ifNotExists: true);

        await using (CamusCommand create = connection.CreateCamusCommand(
            $"CREATE TABLE IF NOT EXISTS {Table} (k STRING NOT NULL, n INT64 NOT NULL, PRIMARY KEY (k))"))
            await create.ExecuteDDLAsync();

        await using (CamusCommand delete = connection.CreateCamusCommand(
            $"DELETE FROM {Table} WHERE k NOT IN ('a1', 'a2', 'a3', 'a4', 'a5')"))
            await delete.ExecuteNonQueryAsync();

        await using (CamusCommand upsert = connection.CreateCamusCommand(
            $"UPSERT INTO {Table} (k, n) VALUES ('a1', 1), ('a2', 2), ('a3', 3), ('a4', 4), ('a5', 5)"))
            Assert.Equal(5, await upsert.ExecuteNonQueryAsync());

        return Table;
    }

    private static async Task<List<string>> ReadKeysAsync(CamusCommand command)
    {
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        List<string> keys = [];
        while (await reader.ReadAsync())
            keys.Add(reader.GetString(0));

        return keys;
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task SqlSetOperationsReturnTheStandardRows(string protocol)
    {
        if (!Enabled)
            return;

        string t = await SeedAsync(protocol);
        await using CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString(protocol)));
        await connection.OpenAsync();

        await using (CamusCommand union = connection.CreateCamusCommand(
            $"SELECT k FROM {t} WHERE n <= @hi UNION SELECT k FROM {t} WHERE n >= @lo ORDER BY k"))
        {
            union.Parameters.Add("@hi", ColumnType.Integer64, 2L);
            union.Parameters.Add("@lo", ColumnType.Integer64, 2L);
            Assert.Equal(["a1", "a2", "a3", "a4", "a5"], await ReadKeysAsync(union));
        }

        await using (CamusCommand all = connection.CreateCamusCommand(
            $"SELECT k FROM {t} WHERE n <= 2 UNION ALL SELECT k FROM {t} WHERE n >= 2 ORDER BY k"))
            Assert.Equal(["a1", "a2", "a2", "a3", "a4", "a5"], await ReadKeysAsync(all));

        await using (CamusCommand intersect = connection.CreateCamusCommand(
            $"SELECT k FROM {t} WHERE n <= 3 INTERSECT SELECT k FROM {t} WHERE n >= 3"))
            Assert.Equal(["a3"], await ReadKeysAsync(intersect));

        await using (CamusCommand except = connection.CreateCamusCommand(
            $"SELECT k FROM {t} EXCEPT SELECT k FROM {t} WHERE n > 1"))
            Assert.Equal(["a1"], await ReadKeysAsync(except));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task AParenthesizedFirstOperandRunsAndPrepares(string protocol)
    {
        if (!Enabled)
            return;

        string t = await SeedAsync(protocol);
        await using CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString(protocol)));
        await connection.OpenAsync();

        string sql = $"(SELECT k FROM {t} ORDER BY k DESC LIMIT 2) UNION ALL SELECT k FROM {t} WHERE n = @n ORDER BY 1";

        await using (CamusCommand inline = connection.CreateCamusCommand(sql))
        {
            inline.Parameters.Add("@n", ColumnType.Integer64, 1L);
            Assert.Equal(["a1", "a4", "a5"], await ReadKeysAsync(inline));
        }

        await using CamusCommand prepared = connection.CreateCamusCommand(sql);
        CamusParameter n = prepared.Parameters.Add("@n", ColumnType.Integer64, 1L);
        await prepared.PrepareAsync();

        Assert.Equal(["a1", "a4", "a5"], await ReadKeysAsync(prepared));

        n.Value = 2L;
        Assert.Equal(["a2", "a4", "a5"], await ReadKeysAsync(prepared));
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task LinqSetOperatorsTranslate(string protocol)
    {
        if (!Enabled)
            return;

        await SeedAsync(protocol);
        await using ItemContext ctx = new(new DbContextOptionsBuilder<ItemContext>().UseCamusDB(ConnString(protocol)).Options);

        IQueryable<Item> low = ctx.Items.Where(x => x.N <= 3);
        IQueryable<Item> high = ctx.Items.Where(x => x.N >= 3);

        Assert.Equal(["a1", "a2", "a3", "a4", "a5"], (await low.Union(high).ToListAsync()).Select(x => x.K).Order());
        Assert.Equal(6, await low.Concat(high).CountAsync());
        Assert.Equal(["a3"], (await low.Intersect(high).ToListAsync()).Select(x => x.K));
        Assert.Equal(["a1", "a2"], (await low.Except(high).ToListAsync()).Select(x => x.K).Order());
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task LinqSetOperatorsCompose(string protocol)
    {
        if (!Enabled)
            return;

        await SeedAsync(protocol);
        await using ItemContext ctx = new(new DbContextOptionsBuilder<ItemContext>().UseCamusDB(ConnString(protocol)).Options);

        IQueryable<Item> low = ctx.Items.Where(x => x.N <= 3);
        IQueryable<Item> high = ctx.Items.Where(x => x.N >= 3);

        // ORDER BY, OFFSET and LIMIT over the combined rows: EF puts the set operation in a derived table.
        Assert.Equal(["a2", "a3"], await low.Union(high).OrderBy(x => x.K).Skip(1).Take(2).Select(x => x.K).ToListAsync());

        // Operands with their own ORDER BY and LIMIT.
        Assert.Equal(["a1", "a5"], (await low.OrderBy(x => x.N).Take(1).Union(high.OrderByDescending(x => x.N).Take(1)).ToListAsync())
            .Select(x => x.K).Order());

        // Mixed operators: EF writes the parentheses that keep the order of evaluation.
        Assert.Equal(["a1", "a2", "a4"], (await low.Union(high).Except(ctx.Items.Where(x => x.K == "a3")).Intersect(ctx.Items.Where(x => x.N != 5))
            .ToListAsync()).Select(x => x.K).Order());

        // A set operation as an IN subquery.
        Assert.Equal(["a1", "a5"], await ctx.Items
            .Where(x => low.Where(y => y.N == 1).Select(y => y.K).Union(high.Where(y => y.N == 5).Select(y => y.K)).Contains(x.K))
            .OrderBy(x => x.K).Select(x => x.K).ToListAsync());

        // A NULL column on one side takes the type of the other side.
        var rows = await low.Where(x => x.N == 1).Select(x => new { x.K, M = (long?)x.N })
            .Union(high.Where(x => x.N == 5).Select(x => new { x.K, M = (long?)null }))
            .ToListAsync();
        Assert.Equal([("a1", (long?)1L), ("a5", null)], rows.OrderBy(r => r.K).Select(r => (r.K, r.M)));
    }

    private sealed class ItemContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Item> Items => Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Item>(b =>
            {
                b.ToTable(Table);
                b.HasKey(x => x.K);
                b.Property(x => x.K).HasColumnName("k");
                b.Property(x => x.N).HasColumnName("n");
            });
    }

    private sealed class Item
    {
        public string K { get; set; } = "";

        public long N { get; set; }
    }
}
