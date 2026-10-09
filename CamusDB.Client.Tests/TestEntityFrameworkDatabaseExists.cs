/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.Client.Tests;

/// <summary>
/// How <see cref="CamusDatabaseCreator.ExistsAsync"/> reads the outcome of its probe, and what
/// <c>MigrateAsync</c> does with the answer. The probe is replaced, so no server is needed.
///
/// <para>The answer decides whether EF sends <c>CREATE DATABASE</c>, a superuser statement. A false
/// "no" stops an account that holds DDL grants on an existing database from migrating it.</para>
/// </summary>
public class TestEntityFrameworkDatabaseExists
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    public static TheoryData<string> ExistingOutcomes => new() { "success", "CADB0517" };

    [Fact]
    public async Task TestProbeSuccessMeansTheDatabaseExists()
    {
        await using ProbeContext ctx = new(() => Task.CompletedTask);

        Assert.True(await ctx.Database.GetService<IRelationalDatabaseCreator>().ExistsAsync());
        Assert.True(ctx.Database.GetService<IRelationalDatabaseCreator>().Exists());
    }

    [Fact]
    public async Task TestDatabaseDoesntExistMeansTheDatabaseIsMissing()
    {
        await using ProbeContext ctx = new(Fail("CADB0010"));

        Assert.False(await ctx.Database.GetService<IRelationalDatabaseCreator>().ExistsAsync());
        Assert.False(ctx.Database.GetService<IRelationalDatabaseCreator>().Exists());
    }

    /// <summary>
    /// The server resolves the database before it checks the privilege, so a refusal means the name
    /// resolved. Answering "no" would send the caller to <c>CREATE DATABASE</c>.
    /// </summary>
    [Fact]
    public async Task TestInsufficientPrivilegeMeansTheDatabaseExists()
    {
        await using ProbeContext ctx = new(Fail("CADB0517"));

        Assert.True(await ctx.Database.GetService<IRelationalDatabaseCreator>().ExistsAsync());
        Assert.True(ctx.Database.GetService<IRelationalDatabaseCreator>().Exists());
    }

    /// <summary>
    /// A fault is not evidence that the database is missing. The old provider answered "no" every
    /// time, which hid a fault behind a create attempt.
    /// </summary>
    [Theory]
    [InlineData("CADB0000")]
    [InlineData("CADB0001")]
    [InlineData("CADB0003")]
    [InlineData("CADB0516")]
    public async Task TestOtherServerErrorsPropagate(string code)
    {
        await using ProbeContext ctx = new(Fail(code));

        CamusException ex = await Assert.ThrowsAsync<CamusException>(
            () => ctx.Database.GetService<IRelationalDatabaseCreator>().ExistsAsync());
        Assert.Equal(code, ex.Code);

        ex = Assert.Throws<CamusException>(() => ctx.Database.GetService<IRelationalDatabaseCreator>().Exists());
        Assert.Equal(code, ex.Code);
    }

    [Fact]
    public async Task TestNonCamusErrorsPropagate()
    {
        await using ProbeContext ctx = new(() => throw new HttpRequestException("connection reset"));

        await Assert.ThrowsAsync<HttpRequestException>(
            () => ctx.Database.GetService<IRelationalDatabaseCreator>().ExistsAsync());
        Assert.Throws<HttpRequestException>(() => ctx.Database.GetService<IRelationalDatabaseCreator>().Exists());
    }

    [Theory]
    [MemberData(nameof(ExistingOutcomes))]
    public async Task TestMigrateDoesNotCreateAnExistingDatabase(string outcome)
    {
        await using ProbeContext ctx = new(outcome == "success" ? () => Task.CompletedTask : Fail(outcome));

        await Assert.ThrowsAsync<StopMigration>(() => ctx.Database.MigrateAsync());

        Assert.Equal(0, ctx.CreateCalls);
    }

    [Fact]
    public async Task TestMigrateCreatesAMissingDatabase()
    {
        await using ProbeContext ctx = new(Fail("CADB0010"));

        await Assert.ThrowsAsync<StopMigration>(() => ctx.Database.MigrateAsync());

        Assert.Equal(1, ctx.CreateCalls);
    }

    [Fact]
    public async Task TestMigrateDoesNotCreateTheDatabaseWhenTheProbeFails()
    {
        await using ProbeContext ctx = new(Fail("CADB0000"));

        CamusException ex = await Assert.ThrowsAsync<CamusException>(() => ctx.Database.MigrateAsync());

        Assert.Equal("CADB0000", ex.Code);
        Assert.Equal(0, ctx.CreateCalls);
    }

    private static Func<Task> Fail(string code) => () => throw new CamusException(code, $"probe failed with {code}");

    /// <summary>Thrown where the migration would first touch the server, after the existence step.</summary>
    private sealed class StopMigration : Exception;

    private sealed class ProbeContext(Func<Task> probe) : DbContext
    {
        public Func<Task> Probe { get; } = probe;

        public int CreateCalls { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder
                .UseCamusDB(ConnString)
                .ReplaceService<IRelationalDatabaseCreator, ProbedDatabaseCreator>()
                .ReplaceService<IHistoryRepository, StoppingHistoryRepository>()
                .ConfigureWarnings(w => w.Ignore(RelationalEventId.PendingModelChangesWarning));
    }

    private sealed class ProbedDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IRelationalConnection connection,
        ICurrentDbContext currentContext)
        : CamusDatabaseCreator(dependencies, connection, currentContext)
    {
        private readonly ProbeContext context = (ProbeContext)currentContext.Context;

        internal override Task ProbeDatabaseAsync(CancellationToken cancellationToken) => context.Probe();

        public override Task CreateAsync(CancellationToken cancellationToken = default)
        {
            context.CreateCalls++;
            return Task.CompletedTask;
        }
    }

    private sealed class StoppingHistoryRepository(HistoryRepositoryDependencies dependencies)
        : CamusHistoryRepository(dependencies)
    {
        // EF 10 creates the history table first, with this script, and takes the lock after it.
        public override string GetCreateIfNotExistsScript() => throw new StopMigration();

        public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(CancellationToken cancellationToken = default)
            => throw new StopMigration();

        public override Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
            => throw new StopMigration();

        public override Task<IReadOnlyList<HistoryRow>> GetAppliedMigrationsAsync(CancellationToken cancellationToken = default)
            => throw new StopMigration();
    }
}
