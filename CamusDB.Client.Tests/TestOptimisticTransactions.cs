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
        await connection.CreateDatabaseAsync(ifNotExists: true);
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
        await connection.CreateDatabaseAsync(ifNotExists: true);
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
    public async Task TestOptimisticReadWriteConflictAbortsStaleCommitter()
    {
        CamusConnection connection = await GetConnection();
        await connection.CreateDatabaseAsync(ifNotExists: true);
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

        // Two independent optimistic transactions on separate connections. Read Committed so tx2's read
        // folds an observation for validation without taking a serializable shared point lock (which would
        // block tx1's write); optimistic conflict detection still fires at commit.
        CamusTransactionOptions options = new()
        {
            IsolationLevel = CamusIsolationLevel.ReadCommitted,
            Locking = CamusLocking.Optimistic,
        };
        CamusConnection connection2 = await GetConnection();
        CamusTransaction tx1 = await connection.BeginTransactionAsync(options);
        CamusTransaction tx2 = await connection2.BeginTransactionAsync(options);

        // tx2 observes the row (folds a read observation of year=2000).
        Assert.Equal(2000, await ReadYear(connection2, tableName, id, tx2));

        // tx1 updates the same row and commits first — nothing blocks, since tx2 holds no lock.
        await UpdateYear(connection, tableName, id, 2001, tx1);
        await tx1.CommitAsync();

        // tx2 continues based on its now-stale read of the row tx1 modified and committed. Optimistic
        // validation aborts it — CamusDB surfaces the stale-read abort at tx2's next operation (the
        // UPDATE's re-read) or at commit; either way tx2 cannot succeed and must replay from BEGIN.
        await Assert.ThrowsAsync<CamusException>(async () =>
        {
            await UpdateYear(connection2, tableName, id, 2002, tx2);
            await tx2.CommitAsync();
        });
    }

    [Fact]
    public async Task TestConnectionStringLockingDefaultAppliesToBeginTransaction()
    {
        CamusConnectionStringBuilder optimisticBuilder =
            new("Endpoint=http://localhost:5095;Database=test;Locking=Optimistic");
        using CamusConnection connection = new(optimisticBuilder);
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync(ifNotExists: true);

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

    private static async Task<long> ReadYear(CamusConnection connection, string tableName, CamusObjectIdValue id, CamusTransaction transaction)
    {
        using CamusCommand select = connection.CreateCamusCommand($"SELECT year FROM {tableName} WHERE id = @id");
        select.Transaction = transaction;
        select.Parameters.Add("@id", ColumnType.Id, id);
        using CamusDataReader reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return reader.GetInt64(0);
    }
}
