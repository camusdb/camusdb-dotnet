/**
 * This file is part of CamusDB
 *
 * Live coverage for Guid parameters against a uuid column. A Guid bound with DbType.Guid used to go on
 * the wire as an object id with 36 characters of text: IN matched no rows, NOT IN matched every row,
 * and = failed on the server. A Guid value now always travels as a Uuid, so every shape below finds
 * its rows. Requires a server on localhost:5095.
 */

using System.Data;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestGuidParametersLive : BaseTest
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private const string TableName = "guid_parameter_live_v1";

    private const int RowCount = 100;

    private static DbContextOptions<AccountContext> Options() =>
        new DbContextOptionsBuilder<AccountContext>().UseCamusDB(ConnString).Options;

    /// <summary>
    /// Inserts <see cref="RowCount"/> rows under a new tag and returns their ids, so each test reads
    /// only its own rows.
    /// </summary>
    private static async Task<(string Tag, List<Guid> Ids)> SeedAsync()
    {
        string tag = Guid.NewGuid().ToString("n");
        List<Guid> ids = Enumerable.Range(0, RowCount).Select(_ => Guid.NewGuid()).ToList();

        await using AccountContext ctx = new(Options());
        await ctx.Database.EnsureCreatedAsync();

        foreach (Guid id in ids)
            ctx.Accounts.Add(new Account { Id = id, Tag = tag });

        await ctx.SaveChangesAsync();
        return (tag, ids);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(50)]
    [InlineData(100)]
    public async Task ContainsOverAGuidList_ReturnsEveryListedRow(int count)
    {
        (string tag, List<Guid> ids) = await SeedAsync();
        List<Guid> wanted = ids.Take(count).ToList();

        await using AccountContext ctx = new(Options());

        List<Guid> found = await ctx.Accounts
            .Where(a => a.Tag == tag && wanted.Contains(a.Id))
            .Select(a => a.Id)
            .ToListAsync();

        Assert.Equal(wanted.ToHashSet(), found.ToHashSet());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(50)]
    [InlineData(100)]
    public async Task NotContainsOverAGuidList_ReturnsEveryOtherRow(int count)
    {
        (string tag, List<Guid> ids) = await SeedAsync();
        List<Guid> excluded = ids.Take(count).ToList();

        await using AccountContext ctx = new(Options());

        List<Guid> found = await ctx.Accounts
            .Where(a => a.Tag == tag && !excluded.Contains(a.Id))
            .Select(a => a.Id)
            .ToListAsync();

        Assert.Equal(ids.Skip(count).ToHashSet(), found.ToHashSet());
    }

    [Fact]
    public async Task EqualityWithADbTypeGuidParameter_FindsTheRow()
    {
        (string tag, List<Guid> ids) = await SeedAsync();

        await using CamusConnection connection = await GetConnection();
        await using CamusCommand cmd = connection.CreateCamusCommand(
            $"SELECT id FROM {TableName} WHERE id = @id AND tag = @tag");
        cmd.Parameters.Add(new CamusParameter { ParameterName = "@id", DbType = DbType.Guid, Value = ids[7] });
        cmd.Parameters.Add("@tag", ColumnType.String, tag);

        List<Guid> found = await ReadIdsAsync(cmd);

        Assert.Equal([ids[7]], found);
    }

    [Fact]
    public async Task InWithDbTypeGuidParameters_FindsTheRows()
    {
        (string tag, List<Guid> ids) = await SeedAsync();

        await using CamusConnection connection = await GetConnection();
        await using CamusCommand cmd = connection.CreateCamusCommand(
            $"SELECT id FROM {TableName} WHERE id IN (@a, @b, @c) AND tag = @tag");
        cmd.Parameters.Add(new CamusParameter { ParameterName = "@a", DbType = DbType.Guid, Value = ids[0] });
        cmd.Parameters.Add(new CamusParameter { ParameterName = "@b", DbType = DbType.Guid, Value = ids[1] });
        cmd.Parameters.Add(new CamusParameter { ParameterName = "@c", DbType = DbType.Guid, Value = Guid.NewGuid() });
        cmd.Parameters.Add("@tag", ColumnType.String, tag);

        List<Guid> found = await ReadIdsAsync(cmd);

        Assert.Equal(new HashSet<Guid> { ids[0], ids[1] }, found.ToHashSet());
    }

    private static async Task<List<Guid>> ReadIdsAsync(CamusCommand cmd)
    {
        List<Guid> ids = [];

        await using CamusDataReader reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ids.Add(reader.GetGuid(0));

        return ids;
    }

    private sealed class AccountContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Account> Accounts => Set<Account>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Account>(b =>
            {
                b.ToTable(TableName);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnName("id").HasColumnType("uuid");
                b.Property(e => e.Tag).HasColumnName("tag").HasMaxLength(64);
            });
        }
    }

    private sealed class Account
    {
        public Guid Id { get; set; }
        public string Tag { get; set; } = "";
    }
}
