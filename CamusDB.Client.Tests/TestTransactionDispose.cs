/**
 * This file is part of CamusDB
 *
 * Offline coverage for the dispose of a transaction, over mocked HTTP: a transaction that is neither
 * committed nor rolled back is rolled back when it is disposed (synchronously or asynchronously), a
 * finished transaction sends nothing more, a deferred transaction that never started sends nothing,
 * and a failed rollback does not escape from Dispose.
 *
 * Endpoint lists are unique per test: pools are process-shared per configuration, so a reused list
 * would inherit another test's rotation position or quarantine.
 */

using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestTransactionDispose
{
    private static string Url(int port, string path) => $"http://localhost:{port}/{path}";

    private static void MockNode(HttpTest httpTest, int port, int rollbackStatus = 200)
    {
        httpTest.ForCallsTo(Url(port, "start-transaction")).RespondWithJson(new { status = "ok", txnIdPT = 10L + port, txnIdCounter = 20u });
        httpTest.ForCallsTo(Url(port, "commit-transaction")).RespondWithJson(new { status = "ok" });
        httpTest.ForCallsTo(Url(port, "execute-sql-non-query")).RespondWithJson(new { status = "ok", rows = 1 });

        if (rollbackStatus == 200)
            httpTest.ForCallsTo(Url(port, "rollback-transaction")).RespondWithJson(new { status = "ok" });
        else
            httpTest.ForCallsTo(Url(port, "rollback-transaction"))
                .RespondWithJson(new { status = "failed", code = "CADB0502", message = "transaction aborted" }, rollbackStatus);
    }

    private static CamusConnection Connect(int port) =>
        new(new CamusConnectionStringBuilder($"Endpoint=http://localhost:{port};Database=test;MaxAutoPrepare=0"));

    private static async Task RunNonQuery(CamusConnection connection, CamusTransaction transaction)
    {
        await using CamusCommand command = connection.CreateCamusCommand("UPDATE t SET a = 1");
        command.Transaction = transaction;
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task DisposeAsyncRollsBackAnOpenTransaction()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9520);
        using CamusConnection connection = Connect(9520);

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await RunNonQuery(connection, transaction);
        await transaction.DisposeAsync();

        httpTest.ShouldHaveCalled(Url(9520, "rollback-transaction")).Times(1);
        httpTest.ShouldNotHaveCalled(Url(9520, "commit-transaction"));
    }

    [Fact]
    public async Task DisposeRollsBackAnOpenTransaction()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9522);
        using CamusConnection connection = Connect(9522);

        using (CamusTransaction transaction = await connection.BeginTransactionAsync())
            await RunNonQuery(connection, transaction);

        httpTest.ShouldHaveCalled(Url(9522, "rollback-transaction")).Times(1);
    }

    [Fact]
    public async Task DisposeAfterACommitSendsNothing()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9524);
        using CamusConnection connection = Connect(9524);

        await using (CamusTransaction transaction = await connection.BeginTransactionAsync())
        {
            await RunNonQuery(connection, transaction);
            await transaction.CommitAsync();
        }

        httpTest.ShouldHaveCalled(Url(9524, "commit-transaction")).Times(1);
        httpTest.ShouldNotHaveCalled(Url(9524, "rollback-transaction"));
    }

    [Fact]
    public async Task DisposeAfterARollbackDoesNotRollBackAgain()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9526);
        using CamusConnection connection = Connect(9526);

        await using (CamusTransaction transaction = await connection.BeginTransactionAsync())
        {
            await RunNonQuery(connection, transaction);
            await transaction.RollbackAsync();
        }

        httpTest.ShouldHaveCalled(Url(9526, "rollback-transaction")).Times(1);
    }

    [Fact]
    public async Task ASecondDisposeDoesNotRollBackAgain()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9528);
        using CamusConnection connection = Connect(9528);

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await transaction.DisposeAsync();
        await transaction.DisposeAsync();
        transaction.Dispose();

        httpTest.ShouldHaveCalled(Url(9528, "rollback-transaction")).Times(1);
    }

    [Fact]
    public async Task DisposeOfADeferredTransactionThatNeverStartedSendsNothing()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9530);
        using CamusConnection connection = Connect(9530);

        CamusTransaction transaction = await connection.BeginTransactionAsync(new CamusTransactionOptions { DeferBegin = true });
        Assert.False(transaction.IsStarted);

        await transaction.DisposeAsync();

        // No BEGIN only to roll it back: nothing exists on the server.
        httpTest.ShouldNotHaveCalled(Url(9530, "start-transaction"));
        httpTest.ShouldNotHaveCalled(Url(9530, "rollback-transaction"));
    }

    [Fact]
    public async Task DisposeOfADeferredTransactionThatStartedRollsItBack()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9532);
        using CamusConnection connection = Connect(9532);

        CamusTransaction transaction = await connection.BeginTransactionAsync(new CamusTransactionOptions { DeferBegin = true });
        await RunNonQuery(connection, transaction);
        Assert.True(transaction.IsStarted);

        await transaction.DisposeAsync();

        httpTest.ShouldHaveCalled(Url(9532, "start-transaction")).Times(1);
        httpTest.ShouldHaveCalled(Url(9532, "rollback-transaction")).Times(1);
    }

    [Fact]
    public async Task AFailedRollbackDoesNotEscapeFromDispose()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9534, rollbackStatus: 500);
        using CamusConnection connection = Connect(9534);

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await RunNonQuery(connection, transaction);

        // The server answers the rollback with an error, for example for a transaction it already aborted.
        await transaction.DisposeAsync();

        httpTest.ShouldHaveCalled(Url(9534, "rollback-transaction"));
    }
}
