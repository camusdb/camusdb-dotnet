/**
 * This file is part of CamusDB
 *
 * Offline coverage for where a transaction starts under learned routing, over mocked HTTP: the
 * deferred BEGIN that the first statement sends to its learned endpoint, the eager BEGIN with routing
 * off, the explicit affinity option, the finalize of an empty transaction, a BEGIN failure surfacing
 * from the first statement, concurrent first statements sharing one BEGIN, and the pin holding every
 * later statement on the started endpoint while its advice still feeds the next transaction.
 *
 * Endpoint lists are unique per test: pools and routers are process-shared per configuration, so a
 * reused list would inherit another test's rotation position, quarantine, or learned routes.
 * Automatic preparation is off in every connection string so the only requests on the wire are the
 * five the assertions count.
 */

using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestTransactionRouting
{
    private static object RoutingJson(string nodeId) => new
    {
        version = 1,
        disposition = "prefer",
        preferredNodeId = nodeId,
        reuseScope = "statementParametersIndependent",
        dependencyToken = "tok",
        maxAgeMs = 5000,
        provenance = "placementHint",
        reason = "singleTableHash",
    };

    private static string Url(int port, string path) => $"http://localhost:{port}/{path}";

    /// <summary>
    /// Mocks one node: BEGIN, COMMIT and ROLLBACK answer ok, and every statement answers ok with
    /// advice naming <paramref name="advertise"/> (or no advice when null).
    /// </summary>
    private static void MockNode(HttpTest httpTest, int port, string? advertise, int startStatus = 200)
    {
        object start = new { status = "ok", txnIdPT = 10L + port, txnIdCounter = 20u };
        httpTest.ForCallsTo(Url(port, "start-transaction")).RespondWithJson(start, startStatus);
        httpTest.ForCallsTo(Url(port, "commit-transaction")).RespondWithJson(new { status = "ok" });
        httpTest.ForCallsTo(Url(port, "rollback-transaction")).RespondWithJson(new { status = "ok" });

        if (advertise is null)
        {
            httpTest.ForCallsTo(Url(port, "execute-sql-non-query")).RespondWithJson(new { status = "ok", rows = 1 });
            httpTest.ForCallsTo(Url(port, "execute-sql-query")).RespondWithJson(
                new { status = "ok", columns = Array.Empty<object>(), rows = Array.Empty<object>() });
            return;
        }

        httpTest.ForCallsTo(Url(port, "execute-sql-non-query")).RespondWithJson(
            new { status = "ok", rows = 1, routing = RoutingJson(advertise) });
        httpTest.ForCallsTo(Url(port, "execute-sql-query")).RespondWithJson(
            new { status = "ok", columns = Array.Empty<object>(), rows = Array.Empty<object>(), routing = RoutingJson(advertise) });
    }

    private static CamusConnectionStringBuilder Routed(int portA, int portB) => new(
        $"Endpoint=http://localhost:{portA},http://localhost:{portB};Database=test;RoutingMode=Learned;MaxAutoPrepare=0;" +
        $"RoutingNodes='camus-a:7070=http://localhost:{portA},camus-b:7070=http://localhost:{portB}'");

    private static async Task RunNonQuery(CamusConnection connection, string sql, CamusTransaction? transaction = null)
    {
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        command.Transaction = transaction;
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }

    [Fact]
    public async Task DeferredBeginStartsOnTheFirstStatementsLearnedEndpoint()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9300, advertise: "camus-b:7070");
        MockNode(httpTest, 9302, advertise: "camus-b:7070");

        using CamusConnection connection = new(Routed(9300, 9302));

        // Warm the statement's route: rotation sends it to A, whose reply names camus-b.
        await RunNonQuery(connection, "UPDATE t SET a = 1");

        CamusTransaction transaction = await connection.BeginTransactionAsync();

        // Nothing was sent yet, and the identity is not minted.
        Assert.False(transaction.IsStarted);
        Assert.Equal(0, transaction.TxnIdPT);
        httpTest.ShouldNotHaveCalled(Url(9300, "start-transaction"));
        httpTest.ShouldNotHaveCalled(Url(9302, "start-transaction"));

        await RunNonQuery(connection, "UPDATE t SET a = 1", transaction);
        await transaction.CommitAsync();

        // BEGIN, the statement and COMMIT all went to the learned leader, B.
        httpTest.ShouldHaveCalled(Url(9302, "start-transaction")).Times(1);
        httpTest.ShouldHaveCalled(Url(9302, "execute-sql-non-query")).Times(1);
        httpTest.ShouldHaveCalled(Url(9302, "commit-transaction")).Times(1);
        httpTest.ShouldNotHaveCalled(Url(9300, "start-transaction"));
        Assert.True(transaction.IsStarted);
        Assert.Equal(10L + 9302, transaction.TxnIdPT);
        Assert.Equal("http://localhost:9302", transaction.Endpoint);
    }

    [Fact]
    public async Task RoutingOffBeginsEagerlyOnRotation()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9304, advertise: null);
        MockNode(httpTest, 9306, advertise: null);

        // No trust map: the Auto default builds no router, so BEGIN behaves exactly as before routing.
        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:9304,http://localhost:9306;Database=test;MaxAutoPrepare=0");
        using CamusConnection connection = new(builder);

        CamusTransaction transaction = await connection.BeginTransactionAsync();

        Assert.True(transaction.IsStarted);
        Assert.Equal(10L + 9304, transaction.TxnIdPT);
        httpTest.ShouldHaveCalled(Url(9304, "start-transaction")).Times(1);

        await RunNonQuery(connection, "UPDATE t SET a = 1", transaction);
        await transaction.RollbackAsync();

        httpTest.ShouldHaveCalled(Url(9304, "execute-sql-non-query")).Times(1);
        httpTest.ShouldHaveCalled(Url(9304, "rollback-transaction")).Times(1);
        httpTest.ShouldNotHaveCalled(Url(9306, "*"));
    }

    [Fact]
    public async Task AffinityStartsBeginAtOnceOnTheNamedStatementsRoute()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9308, advertise: "camus-b:7070");
        MockNode(httpTest, 9310, advertise: "camus-b:7070");

        using CamusConnection connection = new(Routed(9308, 9310));

        // Warm a query route toward B.
        const string hot = "SELECT balance FROM accounts WHERE id = @id";
        await using (CamusCommand warm = connection.CreateCamusCommand(hot))
        {
            warm.Parameters.Add("@id", ColumnType.Integer64, 1);
            await using var reader = await warm.ExecuteReaderAsync();
        }

        CamusTransaction transaction = await connection.BeginTransactionAsync(
            new CamusTransactionOptions { Affinity = hot });

        // The affinity chose B and BEGIN was sent right away, before any statement.
        Assert.True(transaction.IsStarted);
        Assert.Equal("http://localhost:9310", transaction.Endpoint);
        httpTest.ShouldHaveCalled(Url(9310, "start-transaction")).Times(1);
        httpTest.ShouldNotHaveCalled(Url(9308, "start-transaction"));

        // A cold affinity falls back to rotation rather than adding a discovery round trip.
        CamusTransaction cold = await connection.BeginTransactionAsync(
            new CamusTransactionOptions { Affinity = "SELECT 1 FROM never_seen" });
        Assert.True(cold.IsStarted);
        Assert.NotNull(cold.Endpoint);
    }

    [Fact]
    public async Task EmptyTransactionFinalizeSendsBeginThenCommit()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9312, advertise: "camus-b:7070");
        MockNode(httpTest, 9314, advertise: "camus-b:7070");

        using CamusConnection connection = new(Routed(9312, 9314));

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        Assert.False(transaction.IsStarted);

        await transaction.CommitAsync();

        // Rotation chose A; the server saw the same BEGIN-then-COMMIT pair an eager client sends.
        Assert.True(transaction.IsStarted);
        httpTest.ShouldHaveCalled(Url(9312, "start-transaction")).Times(1);
        httpTest.ShouldHaveCalled(Url(9312, "commit-transaction")).Times(1);
        httpTest.ShouldNotHaveCalled(Url(9314, "*"));
    }

    [Fact]
    public async Task BeginFailureSurfacesAtTheFirstStatement()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9316, advertise: "camus-b:7070", startStatus: 500);
        MockNode(httpTest, 9318, advertise: "camus-b:7070", startStatus: 500);

        using CamusConnection connection = new(Routed(9316, 9318));

        // BeginTransaction itself cannot fail: nothing is sent.
        CamusTransaction transaction = await connection.BeginTransactionAsync();

        await using CamusCommand first = connection.CreateCamusCommand("UPDATE t SET a = 1");
        first.Transaction = transaction;
        await Assert.ThrowsAnyAsync<Exception>(() => first.ExecuteNonQueryAsync());

        // The failure is final for this transaction: a second statement does not send a second BEGIN.
        await using CamusCommand second = connection.CreateCamusCommand("UPDATE t SET a = 2");
        second.Transaction = transaction;
        await Assert.ThrowsAnyAsync<Exception>(() => second.ExecuteNonQueryAsync());

        Assert.False(transaction.IsStarted);
        httpTest.ShouldHaveCalled(Url(9316, "start-transaction")).Times(1);
        httpTest.ShouldNotHaveCalled(Url(9316, "execute-sql-non-query"));
        httpTest.ShouldNotHaveCalled(Url(9318, "*"));

        // Nothing was started, so a rollback (the disposing scope's reflex) has nothing to undo and
        // must not throw; a commit still reports that the transaction never began.
        await transaction.RollbackAsync();
        await Assert.ThrowsAnyAsync<Exception>(() => transaction.CommitAsync());
        httpTest.ShouldNotHaveCalled(Url(9316, "rollback-transaction"));
        httpTest.ShouldNotHaveCalled(Url(9316, "commit-transaction"));
    }

    [Fact]
    public async Task ConcurrentFirstStatementsShareOneBegin()
    {
        using HttpTest httpTest = new();
        MockNode(httpTest, 9320, advertise: null);
        MockNode(httpTest, 9322, advertise: null);

        using CamusConnection connection = new(Routed(9320, 9322));

        CamusTransaction transaction = await connection.BeginTransactionAsync();

        Task[] statements = new Task[8];
        for (int i = 0; i < statements.Length; i++)
            statements[i] = RunNonQuery(connection, $"UPDATE t SET a = {i}", transaction);
        await Task.WhenAll(statements);

        // Exactly one BEGIN, and every statement carried its identity to the one endpoint it seated.
        int begins = httpTest.CallLog.Count(call => call.Request.Url.Path == "/start-transaction");
        Assert.Equal(1, begins);

        string endpoint = transaction.Endpoint!;
        httpTest.ShouldHaveCalled(endpoint + "/execute-sql-non-query").Times(statements.Length);
        Assert.True(transaction.IsStarted);
        Assert.NotEqual(0, transaction.TxnIdPT);
    }

    [Fact]
    public async Task StatementsInsideATransactionStayOnItsEndpointButStillTeachTheNextOne()
    {
        using HttpTest httpTest = new();
        // A's replies point at B, B's replies point at A: every statement's learned route names the
        // other node, so a statement that "followed its own route" would visibly leave the pin.
        MockNode(httpTest, 9324, advertise: "camus-b:7070");
        MockNode(httpTest, 9326, advertise: "camus-a:7070");

        using CamusConnection connection = new(Routed(9324, 9326));

        const string first = "UPDATE t SET a = 1";
        const string second = "UPDATE u SET b = 2";

        // Warm: rotation sends `first` to A (learns B) and `second` to B (learns A).
        await RunNonQuery(connection, first);
        await RunNonQuery(connection, second);

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        await RunNonQuery(connection, first, transaction);   // starts the transaction on B
        await RunNonQuery(connection, second, transaction);  // learned A, but the pin is B
        await transaction.CommitAsync();

        httpTest.ShouldHaveCalled(Url(9326, "start-transaction")).Times(1);
        httpTest.ShouldHaveCalled(Url(9326, "execute-sql-non-query")).Times(3);
        httpTest.ShouldHaveCalled(Url(9326, "commit-transaction")).Times(1);
        httpTest.ShouldHaveCalled(Url(9324, "execute-sql-non-query")).Times(1);
        httpTest.ShouldNotHaveCalled(Url(9324, "start-transaction"));

        // B's reply to `first` inside the transaction named camus-a, and that advice was learned: the
        // next transaction whose first statement is `first` starts on A.
        CamusTransaction next = await connection.BeginTransactionAsync();
        await RunNonQuery(connection, first, next);
        await next.RollbackAsync();

        httpTest.ShouldHaveCalled(Url(9324, "start-transaction")).Times(1);
        httpTest.ShouldHaveCalled(Url(9324, "rollback-transaction")).Times(1);
        Assert.Equal("http://localhost:9324", next.Endpoint);
    }
}
