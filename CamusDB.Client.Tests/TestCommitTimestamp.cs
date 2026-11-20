/**
 * This file is part of CamusDB
 *
 * Offline coverage for CamusTransaction.CommitTimestamp: the order and the wire forms of
 * CamusHlcTimestamp, the gRPC CommitReply fields, the REST commit answer (with a value, with null,
 * and from an older server without the field), a rollback, a pipeline commit, and a commit retried
 * after CADB0509.
 *
 * Endpoint lists are unique per test: pools are process-shared per configuration, so a reused list
 * would inherit another test's rotation position or quarantine.
 */

using System.Text.Json;
using CamusDB.Client.Transport.Batching;
using CamusDB.Grpc;
using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestCommitTimestamp
{
    private static readonly CamusHlcTimestamp Sample = new() { N = 2, L = 1_795_124_033_385, C = 7 };

    private static string Url(int port, string path) => $"http://localhost:{port}/{path}";

    private static CamusConnection Connect(int port) =>
        new(new CamusConnectionStringBuilder($"Endpoint=http://localhost:{port};Database=test;MaxAutoPrepare=0"));

    private static void MockNode(HttpTest httpTest, int port, object commitAnswer)
    {
        httpTest.ForCallsTo(Url(port, "start-transaction")).RespondWithJson(new { status = "ok", txnIdPT = 10L + port, txnIdCounter = 20u });
        httpTest.ForCallsTo(Url(port, "commit-transaction")).RespondWithJson(commitAnswer);
        httpTest.ForCallsTo(Url(port, "rollback-transaction")).RespondWithJson(new { status = "ok" });
        httpTest.ForCallsTo(Url(port, "execute-sql-non-query")).RespondWithJson(new { status = "ok", rows = 1 });
    }

    private static async Task RunNonQuery(CamusConnection connection, CamusTransaction transaction)
    {
        await using CamusCommand command = connection.CreateCamusCommand("UPDATE t SET a = 1");
        command.Transaction = transaction;
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

    // ─── CamusHlcTimestamp ──────────────────────────────────────────────────────

    [Fact]
    public void ValuesCompareByPhysicalTimeThenCounterThenNode()
    {
        CamusHlcTimestamp low = new() { N = 9, L = 100, C = 9 };
        CamusHlcTimestamp laterCounter = new() { N = 1, L = 100, C = 10 };
        CamusHlcTimestamp laterTime = new() { N = 0, L = 101, C = 0 };
        CamusHlcTimestamp higherNode = new() { N = 2, L = 101, C = 0 };

        Assert.True(low < laterCounter);
        Assert.True(laterCounter < laterTime);
        Assert.True(laterTime < higherNode);
        Assert.True(higherNode > low);
        Assert.True(low <= low && low >= low);
        Assert.Equal(0, low.CompareTo(new CamusHlcTimestamp { N = 9, L = 100, C = 9 }));

        CamusHlcTimestamp[] sorted = [higherNode, low, laterTime, laterCounter];
        Array.Sort(sorted);
        Assert.Equal([low, laterCounter, laterTime, higherNode], sorted);
    }

    [Fact]
    public void ValuesAreEqualOnlyWhenAllThreePartsAre()
    {
        Assert.Equal(new CamusHlcTimestamp { N = 1, L = 5, C = 2 }, new CamusHlcTimestamp { N = 1, L = 5, C = 2 });
        Assert.NotEqual(new CamusHlcTimestamp { N = 1, L = 5, C = 2 }, new CamusHlcTimestamp { N = 2, L = 5, C = 2 });
        Assert.True(new CamusHlcTimestamp { N = 1, L = 5, C = 2 } == new CamusHlcTimestamp { N = 1, L = 5, C = 2 });
        Assert.NotEqual(0, new CamusHlcTimestamp { N = 1, L = 5, C = 2 }.CompareTo(new CamusHlcTimestamp { N = 2, L = 5, C = 2 }));
    }

    [Fact]
    public void ToStringUsesTheServerFormat()
        => Assert.Equal("HLC(2:1795124033385:7)", Sample.ToString());

    [Fact]
    public void TheJsonFormIsNLC()
    {
        CamusHlcTimestamp read = JsonSerializer.Deserialize(
            """{"n":2,"l":1795124033385,"c":7}""", CamusJsonSerializerContext.Default.CamusHlcTimestamp);
        Assert.Equal(Sample, read);
    }

    // ─── gRPC CommitReply ───────────────────────────────────────────────────────

    [Fact]
    public void ACommitReplyCarriesTheCommitTimestampBesideTheCausalToken()
    {
        BatchCommitResult result = BatchCommitResult.FromReply(new CommitReply
        {
            CausalTokenN = 1, CausalTokenL = 1_795_124_033_390, CausalTokenC = 0,
            CommitTimestampN = 2, CommitTimestampL = 1_795_124_033_385, CommitTimestampC = 7,
        });

        Assert.Equal(1_795_124_033_390, result.Token.L);
        Assert.Equal(Sample, result.CommitTimestamp);
    }

    [Fact]
    public void AZeroCommitTimestampAndAnOlderServerBothReadAsNull()
    {
        // A transaction that wrote nothing: the server sends the causal token and zero commit fields.
        // An older server sends no commit fields, which protobuf reads as zero too.
        Assert.Null(BatchCommitResult.FromReply(new CommitReply { CausalTokenN = 1, CausalTokenL = 9 }).CommitTimestamp);
        Assert.Null(BatchCommitResult.FromReply(new CommitReply()).CommitTimestamp);
    }

    [Fact]
    public void TheCounterTravelsAsAnUnsignedValueInAnInt64()
    {
        // The server writes C as (long)(uint)C, so a counter above int.MaxValue stays positive.
        BatchCommitResult result = BatchCommitResult.FromReply(new CommitReply
        {
            CommitTimestampN = 1, CommitTimestampL = 5, CommitTimestampC = uint.MaxValue,
        });

        Assert.Equal(uint.MaxValue, result.CommitTimestamp!.Value.C);
    }

    // ─── REST ───────────────────────────────────────────────────────────────────

    [Fact]
    public async Task ARestCommitSetsTheCommitTimestamp()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9700, new
        {
            status = "ok",
            causalToken = new { n = 1, l = 1_795_124_033_390L, c = 0u },
            commitTimestamp = new { n = 2, l = 1_795_124_033_385L, c = 7u },
        });
        using CamusConnection connection = Connect(9700);

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await RunNonQuery(connection, transaction);
        Assert.Null(transaction.CommitTimestamp);

        await transaction.CommitAsync();

        Assert.Equal(Sample, transaction.CommitTimestamp);
    }

    [Fact]
    public async Task ARestCommitThatWroteNothingLeavesItNull()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9702, new { status = "ok", causalToken = new { n = 1, l = 9L, c = 0u }, commitTimestamp = (object?)null });
        using CamusConnection connection = Connect(9702);

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await transaction.CommitAsync();

        Assert.Null(transaction.CommitTimestamp);
    }

    [Fact]
    public async Task ACommitAnswerFromAnOlderServerLeavesItNull()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9704, new { status = "ok" });
        using CamusConnection connection = Connect(9704);

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await RunNonQuery(connection, transaction);
        await transaction.CommitAsync();

        Assert.Null(transaction.CommitTimestamp);
    }

    [Fact]
    public async Task ARollbackLeavesItNull()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9706, new { status = "ok", commitTimestamp = new { n = 2, l = 5L, c = 7u } });
        using CamusConnection connection = Connect(9706);

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await RunNonQuery(connection, transaction);
        await transaction.RollbackAsync();

        Assert.Null(transaction.CommitTimestamp);
        httpTest.ShouldNotHaveCalled(Url(9706, "commit-transaction"));
    }

    [Fact]
    public async Task APipelineCommitSetsIt()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9708, new { status = "ok", commitTimestamp = new { n = 2, l = 1_795_124_033_385L, c = 7u } });
        using CamusConnection connection = Connect(9708);

        CamusTransaction transaction = await connection.BeginTransactionAsync(new CamusTransactionOptions { DeferBegin = true });
        CamusPipeline pipeline = transaction.CreatePipeline();
        await using CamusCommand command = connection.CreateCamusCommand("UPDATE t SET a = 1");
        Task<int> update = pipeline.QueueNonQuery(command);
        await pipeline.CommitAsync();

        Assert.Equal(1, await update);
        Assert.Equal(Sample, transaction.CommitTimestamp);
    }

    // ─── CADB0509 retry ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ACommitRetriedAfterAnUnresolvedOutcomeReportsTheValueOfTheAnswer()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:9710;Database=test;MaxAutoPrepare=0");
        TestTelemetry.FakeTransport transport = new() { FinalizeFailures = 2, CommitTimestamp = Sample };
        builder.UseTransport(transport);
        using CamusConnection connection = new(builder);
        connection.Open();

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await transaction.CommitAsync();

        Assert.Equal(0, transport.FinalizeFailures);
        Assert.Equal(Sample, transaction.CommitTimestamp);
    }
}
