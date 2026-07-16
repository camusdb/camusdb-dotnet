/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client.Tests;

/// <summary>
/// Server-backed coverage of the optimistic-locking / isolation-selection surface. Requires a live
/// CamusDB node (same as <see cref="TestTransactions"/>).
/// </summary>
public class TestOptimisticTransactions : BaseTest
{
    [Fact]
    public async Task TestBeginOptimisticTransactionAndCommit()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);
        CamusObjectIdValue id = CamusObjectIdGenerator.Generate();

        CamusTransaction transaction = await connection.BeginTransactionAsync(CamusTransactionOptions.Optimistic);
        Assert.Equal(CamusLocking.Optimistic, transaction.Options.Locking);

        using CamusCommand insert = connection.CreateInsertCommand(tableName);
        insert.Transaction = transaction;
        insert.Parameters.Add("id", ColumnType.Id, id);
        insert.Parameters.Add("name", ColumnType.String, "optimist");
        insert.Parameters.Add("type", ColumnType.String, "mechanical");
        insert.Parameters.Add("year", ColumnType.Integer64, 2000);

        Assert.Equal(1, await insert.ExecuteNonQueryAsync());

        await transaction.CommitAsync();

        // The committed row is visible afterwards.
        using CamusCommand select = connection.CreateCamusCommand($"SELECT name FROM {tableName} WHERE id = @id");
        select.Parameters.Add("@id", ColumnType.Id, id);
        using CamusDataReader reader = await select.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.Equal("optimist", reader.GetString(0));
    }

    [Fact]
    public async Task TestBeginReadCommittedTransaction()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        CamusTransaction transaction = await connection.BeginTransactionAsync(
            new CamusTransactionOptions { IsolationLevel = CamusIsolationLevel.ReadCommitted });

        Assert.Equal(System.Data.IsolationLevel.ReadCommitted, transaction.IsolationLevel);

        using CamusCommand insert = connection.CreateInsertCommand(tableName);
        insert.Transaction = transaction;
        insert.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        insert.Parameters.Add("name", ColumnType.String, "rc");
        insert.Parameters.Add("type", ColumnType.String, "mechanical");
        insert.Parameters.Add("year", ColumnType.Integer64, 2000);

        Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        await transaction.CommitAsync();
    }

    [Fact]
    public async Task TestOptimisticWriteWriteConflictAbortsOneCommitter()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);
        CamusObjectIdValue id = CamusObjectIdGenerator.Generate();

        // Seed a row (autocommit).
        using (CamusCommand seed = connection.CreateInsertCommand(tableName))
        {
            seed.Parameters.Add("id", ColumnType.Id, id);
            seed.Parameters.Add("name", ColumnType.String, "seed");
            seed.Parameters.Add("type", ColumnType.String, "mechanical");
            seed.Parameters.Add("year", ColumnType.Integer64, 2000);
            Assert.Equal(1, await seed.ExecuteNonQueryAsync());
        }

        CamusTransaction tx1 = await connection.BeginTransactionAsync(CamusTransactionOptions.Optimistic);
        CamusTransaction tx2 = await connection.BeginTransactionAsync(CamusTransactionOptions.Optimistic);

        await UpdateYear(connection, tableName, id, 2001, tx1);
        await UpdateYear(connection, tableName, id, 2002, tx2);

        // First committer wins; the second optimistic writer conflicts on the same key at commit.
        await tx1.CommitAsync();
        await Assert.ThrowsAsync<CamusException>(() => tx2.CommitAsync());
    }

    [Fact]
    public async Task TestConnectionStringLockingDefaultAppliesToBeginTransaction()
    {
        CamusConnectionStringBuilder optimisticBuilder =
            new("Endpoint=http://localhost:5095;Database=test;Locking=Optimistic");
        using CamusConnection connection = new(optimisticBuilder);
        await connection.OpenAsync();

        CamusTransaction transaction = await connection.BeginTransactionAsync();

        // No per-transaction option was passed, so the connection-string default takes effect.
        Assert.Equal(CamusLocking.Optimistic, transaction.Options.Locking);

        await transaction.RollbackAsync();
    }

    private static async Task UpdateYear(CamusConnection connection, string tableName, CamusObjectIdValue id, long year, CamusTransaction transaction)
    {
        using CamusCommand update = connection.CreateCamusCommand($"UPDATE {tableName} SET year = @year WHERE id = @id");
        update.Transaction = transaction;
        update.Parameters.Add("@year", ColumnType.Integer64, year);
        update.Parameters.Add("@id", ColumnType.Id, id);
        await update.ExecuteNonQueryAsync();
    }
}
