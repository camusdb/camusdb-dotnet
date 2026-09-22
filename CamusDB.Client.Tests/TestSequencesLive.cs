/**
 * This file is part of CamusDB
 *
 * Live coverage for sequences: the DDL routing of the driver, EnsureCreated with a sequence-backed
 * column, the UseSequence and UseHiLo value generators, and Database.NextSequenceValue. Requires a
 * server that supports sequences (0.13.2 or later), so the tests are opt-in: set
 * CAMUSDB_TEST_SEQUENCES=true. The endpoint comes from CAMUSDB_TEST_ENDPOINT, default localhost:5095.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestSequencesLive
{
    private static bool Enabled
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_SEQUENCES"), "true", StringComparison.OrdinalIgnoreCase);

    private static string ConnString
        => $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095"};Database=test";

    [Fact]
    public async Task SequenceDdlRoundTrip()
    {
        if (!Enabled)
            return;

        string name = "seq_ddl_" + Guid.NewGuid().ToString("n");
        string renamed = name + "_r";

        await using CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString));
        await connection.OpenAsync();

        await ExecAsync(connection, $"CREATE SEQUENCE `{name}` START WITH 100 INCREMENT BY 5");
        Assert.Equal(100L, await ScalarAsync(connection, $"SELECT nextval('{name}')"));
        Assert.Equal(105L, await ScalarAsync(connection, $"SELECT nextval('{name}')"));

        await ExecAsync(connection, $"ALTER SEQUENCE `{name}` INCREMENT BY 10");
        await ExecAsync(connection, $"ALTER SEQUENCE `{name}` RENAME TO `{renamed}`");
        Assert.Equal(115L, await ScalarAsync(connection, $"SELECT nextval('{renamed}')"));

        await ExecAsync(connection, $"DROP SEQUENCE `{renamed}`");
        await Assert.ThrowsAsync<CamusException>(() => ScalarAsync(connection, $"SELECT nextval('{renamed}')"));
    }

    [Fact]
    public async Task UseSequenceAssignsValuesOnAdd()
    {
        if (!Enabled)
            return;

        string suffix = Guid.NewGuid().ToString("n")[..12];
        await using (TicketContext setup = new(Options(), suffix))
            await setup.Database.EnsureCreatedAsync();

        List<Ticket> tickets = [];
        await using (TicketContext ctx = new(Options(), suffix))
        {
            for (int i = 0; i < 5; i++)
            {
                Ticket ticket = new() { Id = CamusObjectId(), Subject = "t" + i };
                await ctx.Tickets.AddAsync(ticket);
                // The value is drawn when the entity is added, before SaveChanges.
                Assert.NotEqual(0, ticket.Number);
                tickets.Add(ticket);
            }

            await ctx.SaveChangesAsync();
        }

        Assert.Equal([1, 2, 3, 4, 5], tickets.Select(t => t.Number).Order().ToArray());

        await using (TicketContext ctx = new(Options(), suffix))
        {
            List<long> stored = await ctx.Tickets.Select(t => t.Number).OrderBy(n => n).ToListAsync();
            Assert.Equal([1L, 2L, 3L, 4L, 5L], stored);
        }
    }

    [Fact]
    public async Task UseHiLoHandsOutBlocks()
    {
        if (!Enabled)
            return;

        string suffix = Guid.NewGuid().ToString("n")[..12];
        await using (TicketContext setup = new(Options(), suffix))
            await setup.Database.EnsureCreatedAsync();

        List<Ticket> tickets = [];
        await using (TicketContext ctx = new(Options(), suffix))
        {
            for (int i = 0; i < 25; i++)
            {
                Ticket ticket = new() { Id = CamusObjectId(), Subject = "h" + i };
                ctx.Tickets.Add(ticket);
                tickets.Add(ticket);
            }

            await ctx.SaveChangesAsync();
        }

        // Block size 10: nextval gives 1, 11, 21, and each value opens a block of ten.
        List<int> batches = tickets.Select(t => t.Batch).ToList();
        Assert.Equal(25, batches.Distinct().Count());
        Assert.Equal(Enumerable.Range(1, 25).ToList(), batches.Order().ToList());

        await using (TicketContext ctx = new(Options(), suffix))
            Assert.Equal(31L, await ctx.Database.NextSequenceValueAsync(BatchSequence(suffix)));
    }

    [Fact]
    public async Task ColumnDefaultDrawsFromTheSequence()
    {
        if (!Enabled)
            return;

        string suffix = Guid.NewGuid().ToString("n")[..12];
        await using (TicketContext setup = new(Options(), suffix))
        {
            await setup.Database.EnsureCreatedAsync();
            Assert.Equal(1L, setup.Database.NextSequenceValue(NumberSequence(suffix)));
        }

        // An INSERT in SQL that omits the column takes the DEFAULT (nextval('…')) of the column.
        await using CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString));
        await connection.OpenAsync();
        await ExecAsync(connection,
            $"INSERT INTO `tickets_{suffix}` (`Id`, `Subject`, `Batch`) VALUES ('{CamusObjectId()}', 'raw', 0)");

        Assert.Equal(2L, await ScalarAsync(connection, $"SELECT `Number` FROM `tickets_{suffix}` WHERE `Subject` = 'raw'"));
    }

    private static string NumberSequence(string suffix) => "ticket_no_" + suffix;

    private static string BatchSequence(string suffix) => "ticket_batch_" + suffix;

    private static DbContextOptions<TicketContext> Options()
        => new DbContextOptionsBuilder<TicketContext>().UseCamusDB(ConnString).Options;

    private static string CamusObjectId() => new CamusObjectIdValueGenerator().Next(null!);

    private static async Task ExecAsync(CamusConnection connection, string sql)
    {
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(CamusConnection connection, string sql)
    {
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        return await command.ExecuteScalarAsync();
    }

    public class Ticket
    {
        public string Id { get; set; } = "";

        public long Number { get; set; }

        public int Batch { get; set; }

        public string Subject { get; set; } = "";
    }

    private class TicketContext(DbContextOptions options, string suffix) : DbContext(options)
    {
        public DbSet<Ticket> Tickets => Set<Ticket>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Ticket>(b =>
            {
                b.ToTable("tickets_" + suffix);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Number).UseSequence(NumberSequence(suffix));
                b.Property(e => e.Batch).UseHiLo(BatchSequence(suffix));
            });
        }
    }
}
