/**
 * This file is part of CamusDB
 *
 * Offline coverage for CamusPipeline, the public surface, over mocked HTTP (REST has no frame, so a
 * pipeline costs one request per step there, which is also what the mock can count): the BEGIN of a
 * deferred transaction rides the first send, every queued statement gets its own result, a commit
 * happens only when every statement succeeded, a failed statement fails the statements after it and
 * the commit, and the transaction is usable afterwards exactly as after a failed lone statement.
 */

using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestPipeline
{
    private static string Url(int port, string path) => $"http://localhost:{port}/{path}";

    private static CamusConnectionStringBuilder Builder(int port)
        => new($"Endpoint=http://localhost:{port};Database=test;MaxAutoPrepare=0");

    private static void MockNode(HttpTest httpTest, int port)
    {
        httpTest.ForCallsTo(Url(port, "start-transaction")).RespondWithJson(new { status = "ok", txnIdPT = 77L, txnIdCounter = 3u });
        httpTest.ForCallsTo(Url(port, "commit-transaction")).RespondWithJson(new { status = "ok" });
        httpTest.ForCallsTo(Url(port, "rollback-transaction")).RespondWithJson(new { status = "ok" });
        httpTest.ForCallsTo(Url(port, "execute-sql-non-query")).RespondWithJson(new { status = "ok", rows = 1 });
        httpTest.ForCallsTo(Url(port, "execute-sql-query")).RespondWithJson(new
        {
            status = "ok",
            columns = new[] { new { name = "balance", type = (int)ColumnType.Integer64 } },
            rows = new[] { new object[] { 42 } },
        });
    }

    private static int CallsTo(HttpTest httpTest, int port, string path)
        => httpTest.CallLog.Count(call => call.Request.Url.ToString().StartsWith(Url(port, path), StringComparison.Ordinal));

    [Fact]
    public async Task ADeferredTransactionBeginsInsideItsFirstSendAndCommitsInsideItsLast()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9400);

        using CamusConnection connection = new(Builder(9400));
        CamusTransaction tx = await connection.BeginTransactionAsync(new CamusTransactionOptions { DeferBegin = true });
        Assert.False(tx.IsStarted, "DeferBegin sends no BEGIN from BeginTransaction");
        Assert.Equal(0, CallsTo(httpTest, 9400, "start-transaction"));

        CamusPipeline pipeline = tx.CreatePipeline();
        Task<CamusDataReader> readA = pipeline.QueueReader(connection.CreateSelectCommand("SELECT balance FROM t WHERE id = 1"));
        Task<CamusDataReader> readB = pipeline.QueueReader(connection.CreateSelectCommand("SELECT balance FROM t WHERE id = 2"));
        Assert.Equal(2, pipeline.Count);

        await pipeline.SendAsync();

        Assert.True(tx.IsStarted);
        Assert.Equal(77L, tx.TxnIdPT);
        Assert.Equal(1, CallsTo(httpTest, 9400, "start-transaction"));
        Assert.Equal(2, CallsTo(httpTest, 9400, "execute-sql-query"));
        Assert.Equal(0, pipeline.Count);

        using CamusDataReader a = await readA;
        Assert.True(await a.ReadAsync());
        Assert.Equal(42L, a.GetInt64(0));
        using CamusDataReader b = await readB;
        Assert.True(await b.ReadAsync());

        Task<int> updateA = pipeline.QueueNonQuery(connection.CreateCamusCommand("UPDATE t SET balance = 1 WHERE id = 1"));
        Task<int> updateB = pipeline.QueueNonQuery(connection.CreateCamusCommand("UPDATE t SET balance = 2 WHERE id = 2"));
        await pipeline.CommitAsync();

        Assert.Equal(1, await updateA);
        Assert.Equal(1, await updateB);
        Assert.Equal(2, CallsTo(httpTest, 9400, "execute-sql-non-query"));
        Assert.Equal(1, CallsTo(httpTest, 9400, "commit-transaction"));

        // REST has no frame: one request per step, which the pipeline reports honestly.
        Assert.Equal(6, pipeline.Exchanges);

        // Committed by the pipeline: a rollback from a disposing scope is a quiet no-op, a second
        // commit is refused here rather than asked of the server.
        await tx.RollbackAsync();
        await Assert.ThrowsAsync<InvalidOperationException>(() => tx.CommitAsync());
        Assert.Equal(0, CallsTo(httpTest, 9400, "rollback-transaction"));
    }

    [Fact]
    public async Task AFailedStatementFailsWhatFollowsAndTheCommitAndLeavesTheTransactionRollbackable()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo(Url(9402, "start-transaction")).RespondWithJson(new { status = "ok", txnIdPT = 77L, txnIdCounter = 3u });
        httpTest.ForCallsTo(Url(9402, "rollback-transaction")).RespondWithJson(new { status = "ok" });
        httpTest.ForCallsTo(Url(9402, "commit-transaction")).RespondWithJson(new { status = "ok" });
        // The statements, in call order: the first succeeds, the second fails, nothing else is sent.
        httpTest
            .RespondWithJson(new { status = "ok", rows = 1 })
            .RespondWithJson(new { status = "failed", code = "CADB0101", message = "boom" }, 500);

        using CamusConnection connection = new(Builder(9402));
        CamusTransaction tx = await connection.BeginTransactionAsync(new CamusTransactionOptions { DeferBegin = true });
        CamusPipeline pipeline = tx.CreatePipeline();

        Task<int> ok = pipeline.QueueNonQuery(connection.CreateCamusCommand("UPDATE t SET a = 1"));
        Task<int> bad = pipeline.QueueNonQuery(connection.CreateCamusCommand("UPDATE t SET a = 'boom'"));
        Task<int> after = pipeline.QueueNonQuery(connection.CreateCamusCommand("UPDATE t SET a = 3"));

        CamusException thrown = await Assert.ThrowsAsync<CamusException>(() => pipeline.CommitAsync());
        Assert.Equal("CADB0101", thrown.Code);

        Assert.Equal(1, await ok);
        Assert.Equal("CADB0101", (await Assert.ThrowsAsync<CamusException>(() => bad)).Code);
        Assert.Equal("CADB0101", (await Assert.ThrowsAsync<CamusException>(() => after)).Code);

        Assert.Equal(0, CallsTo(httpTest, 9402, "commit-transaction"));
        Assert.Equal(2, CallsTo(httpTest, 9402, "execute-sql-non-query"));

        // The server ended the transaction with the failure; the client's rollback is the usual no-op.
        await tx.RollbackAsync();
        Assert.Equal(1, CallsTo(httpTest, 9402, "rollback-transaction"));
    }

    [Fact]
    public async Task AFailedBeginSurfacesOnEverythingAndARollbackIsQuiet()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo(Url(9404, "start-transaction"))
            .RespondWithJson(new { status = "failed", code = "CADB0010", message = "no such database" }, 404);
        httpTest.ForCallsTo(Url(9404, "rollback-transaction")).RespondWithJson(new { status = "ok" });
        httpTest.ForCallsTo(Url(9404, "execute-sql-non-query")).RespondWithJson(new { status = "ok", rows = 1 });

        using CamusConnection connection = new(Builder(9404));
        CamusTransaction tx = await connection.BeginTransactionAsync(new CamusTransactionOptions { DeferBegin = true });
        CamusPipeline pipeline = tx.CreatePipeline();
        Task<int> update = pipeline.QueueNonQuery(connection.CreateCamusCommand("UPDATE t SET a = 1"));

        CamusException thrown = await Assert.ThrowsAsync<CamusException>(() => pipeline.CommitAsync());
        Assert.Equal("CADB0010", thrown.Code);
        Assert.Equal("CADB0010", (await Assert.ThrowsAsync<CamusException>(() => update)).Code);
        Assert.False(tx.IsStarted);
        Assert.Equal(0, CallsTo(httpTest, 9404, "execute-sql-non-query"));

        await tx.RollbackAsync();
        Assert.Equal(0, CallsTo(httpTest, 9404, "rollback-transaction"));

        // A BEGIN that failed stays failed: the next statement reports it instead of beginning anew.
        await using CamusCommand later = connection.CreateCamusCommand("UPDATE t SET a = 2");
        later.Transaction = tx;
        Assert.Equal("CADB0010", (await Assert.ThrowsAsync<CamusException>(() => later.ExecuteNonQueryAsync())).Code);
    }

    [Fact]
    public async Task AnAlreadyBegunTransactionSendsNoSecondBeginAndACommandCanRunBetweenSends()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9406);

        using CamusConnection connection = new(Builder(9406));
        CamusTransaction tx = await connection.BeginTransactionAsync();
        Assert.True(tx.IsStarted);

        CamusPipeline pipeline = tx.CreatePipeline();
        Task<int> first = pipeline.QueueNonQuery(connection.CreateCamusCommand("UPDATE t SET a = 1"));
        await pipeline.SendAsync();
        Assert.Equal(1, await first);

        await using CamusCommand between = connection.CreateCamusCommand("UPDATE t SET a = 2");
        between.Transaction = tx;
        Assert.Equal(1, await between.ExecuteNonQueryAsync());

        Task<CamusDataReader> read = pipeline.QueueReader(connection.CreateCamusCommand("INSERT INTO t (a) VALUES (3) RETURNING a"));
        await pipeline.CommitAsync();
        using CamusDataReader reader = await read;
        Assert.Equal(1, reader.RecordsAffected);

        Assert.Equal(1, CallsTo(httpTest, 9406, "start-transaction"));
        Assert.Equal(3, CallsTo(httpTest, 9406, "execute-sql-non-query"));
        Assert.Equal(1, CallsTo(httpTest, 9406, "commit-transaction"));
    }

    [Fact]
    public async Task WhatCannotBePipelinedIsRefusedAtQueueTimeOrAtSend()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9408);

        using CamusConnection connection = new(Builder(9408));
        CamusTransaction tx = await connection.BeginTransactionAsync();
        CamusTransaction other = await connection.BeginTransactionAsync();
        CamusPipeline pipeline = tx.CreatePipeline();

        Assert.Throws<NotSupportedException>(() => { _ = pipeline.QueueNonQuery(connection.CreateInsertCommand("t")); });

        CamusCommand foreign = connection.CreateCamusCommand("UPDATE t SET a = 1");
        foreign.Transaction = other;
        Assert.Throws<InvalidOperationException>(() => { _ = pipeline.QueueNonQuery(foreign); });

        Task<int> ddl = pipeline.QueueNonQuery(connection.CreateCamusCommand("CREATE TABLE x (id INT64)"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => pipeline.SendAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => ddl);

        // An empty send is nothing; an empty commit is still the commit.
        await pipeline.SendAsync();
        await pipeline.CommitAsync();
        Assert.Equal(1, CallsTo(httpTest, 9408, "commit-transaction"));
    }
}
