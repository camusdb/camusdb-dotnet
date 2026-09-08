/**
 * This file is part of CamusDB
 *
 * Offline coverage for learned statement routing: connection-string parsing of the routing keys,
 * the trust-map and quarantine rules of the router, the revision-guarded TTL cache, and the REST
 * flow end to end over mocked HTTP — negotiation on the request, learning from the response, and
 * the transaction pin overriding every learned route. No server is required.
 *
 * Endpoint lists are unique per test: pools and routers are process-shared per configuration, so a
 * reused list would inherit another test's rotation position, quarantine, or learned routes.
 */

using Flurl.Http.Testing;
using CamusDB.Client.Transport;

namespace CamusDB.Client.Tests;

public class TestStatementRouting
{
    private static CamusRoutingAdvice Prefer(string nodeId, int maxAgeMs = 5_000) => new(
        version: 1,
        disposition: CamusRoutingDisposition.Prefer,
        preferredNodeId: nodeId,
        parametersIndependentScope: true,
        dependencyToken: "tok",
        maxAgeMs: maxAgeMs,
        reason: "singleTableHash");

    // ─── Route cache ──────────────────────────────────────────────────────────

    [Fact]
    public void CacheLearnsGetsAndExpires()
    {
        CamusStatementRouteCache cache = new(maxEntries: 16, maxBytes: 1 << 20);
        CamusStatementRouteKey key = new("db", "select 1", CamusRouteOpKind.Query);

        cache.Learn(key, "node-b", "tok", expiresAt: 1_000, observedRevision: 0, now: 0);

        Assert.Equal("node-b", cache.TryGet(key, now: 999, out long revision));
        Assert.Equal(1, revision);

        Assert.Null(cache.TryGet(key, now: 1_000, out _));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void LateLearnAndLateClearLoseToNewerRevisions()
    {
        CamusStatementRouteCache cache = new(16, 1 << 20);
        CamusStatementRouteKey key = new("db", "select 1", CamusRouteOpKind.Query);

        // A fast response learned node-b; a slower one that observed the pre-learn state must lose.
        cache.Learn(key, "node-b", null, 10_000, observedRevision: 0, now: 0);
        cache.Learn(key, "node-a", null, 10_000, observedRevision: 0, now: 1);
        Assert.Equal("node-b", cache.TryGet(key, 2, out long current));

        // Same rule for clears: a stale one is discarded, a current one removes the route.
        cache.Clear(key, observedRevision: 0);
        Assert.Equal("node-b", cache.TryGet(key, 3, out current));

        cache.Clear(key, observedRevision: current);
        Assert.Null(cache.TryGet(key, 4, out _));
    }

    [Fact]
    public void EntryCapEvictsLeastRecentlyTouched()
    {
        CamusStatementRouteCache cache = new(maxEntries: 2, maxBytes: 1 << 20);

        cache.Learn(new("db", "s1", CamusRouteOpKind.Query), "n", null, 100_000, 0, now: 0);
        cache.Learn(new("db", "s2", CamusRouteOpKind.Query), "n", null, 100_000, 0, now: 1);
        Assert.Equal("n", cache.TryGet(new("db", "s1", CamusRouteOpKind.Query), 5, out _));

        cache.Learn(new("db", "s3", CamusRouteOpKind.Query), "n", null, 100_000, 0, now: 6);

        Assert.Equal(2, cache.Count);
        Assert.Null(cache.TryGet(new("db", "s2", CamusRouteOpKind.Query), 7, out _));
    }

    [Fact]
    public void QueryAndNonQueryAreDistinctRouteIdentities()
    {
        CamusStatementRouteCache cache = new(16, 1 << 20);
        cache.Learn(new("db", "update t set a=1", CamusRouteOpKind.NonQuery), "n", null, 1_000, 0, 0);

        Assert.Null(cache.TryGet(new("db", "update t set a=1", CamusRouteOpKind.Query), 1, out _));
    }

    // ─── Router ───────────────────────────────────────────────────────────────

    [Fact]
    public void RouterMapsLearnedNodeToConfiguredEndpoint()
    {
        CamusEndpointPool pool = new("http://a:9001,http://b:9001");
        CamusStatementRouter router = new(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["node-b"] = "http://b:9001" },
            pool, maxHintAgeMs: 5_000);

        long now = 0;
        router.Clock = () => Volatile.Read(ref now);

        Assert.Null(router.SelectEndpoint("db", "select 1", CamusRouteOpKind.Query, out long observed));
        router.Learn("db", "select 1", CamusRouteOpKind.Query, Prefer("node-b"), observed);

        Assert.Equal("http://b:9001", router.SelectEndpoint("db", "select 1", CamusRouteOpKind.Query, out _));

        // TTL is monotonic from receipt: at the deadline the route is gone.
        Volatile.Write(ref now, 5_000);
        Assert.Null(router.SelectEndpoint("db", "select 1", CamusRouteOpKind.Query, out _));
    }

    [Fact]
    public void RouterIgnoresUnusableAdvice()
    {
        CamusEndpointPool pool = new("http://a:9003,http://b:9003");
        CamusStatementRouter router = new(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["node-b"] = "http://b:9003" },
            pool, maxHintAgeMs: 5_000);

        // Unmapped node identity.
        router.Learn("db", "s", CamusRouteOpKind.Query, Prefer("node-zz"), 0);
        Assert.Equal(0, router.LearnedRouteCount);

        // Unknown metadata version.
        CamusRoutingAdvice unknownVersion = new(2, CamusRoutingDisposition.Prefer, "node-b", true, null, 5_000, null);
        router.Learn("db", "s", CamusRouteOpKind.Query, unknownVersion, 0);
        Assert.Equal(0, router.LearnedRouteCount);

        // A reuse scope the driver does not recognize must never be treated as parameter-independent.
        CamusRoutingAdvice unknownScope = new(1, CamusRoutingDisposition.Prefer, "node-b", false, null, 5_000, null);
        router.Learn("db", "s", CamusRouteOpKind.Query, unknownScope, 0);
        Assert.Equal(0, router.LearnedRouteCount);

        // Non-positive TTL.
        router.Learn("db", "s", CamusRouteOpKind.Query, Prefer("node-b", maxAgeMs: 0), 0);
        Assert.Equal(0, router.LearnedRouteCount);
    }

    [Fact]
    public void RouterSkipsQuarantinedEndpoints()
    {
        CamusEndpointPool pool = new("http://a:9005,http://b:9005");
        CamusStatementRouter router = new(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["node-b"] = "http://b:9005" },
            pool, maxHintAgeMs: 60_000);

        router.Learn("db", "s", CamusRouteOpKind.Query, Prefer("node-b", maxAgeMs: 60_000), 0);
        Assert.Equal("http://b:9005", router.SelectEndpoint("db", "s", CamusRouteOpKind.Query, out _));

        // A learned route pointing at a node that stopped answering falls back to rotation.
        pool.MarkUnreachable("http://b:9005");
        Assert.Null(router.SelectEndpoint("db", "s", CamusRouteOpKind.Query, out _));
    }

    [Fact]
    public void ClearForgetsTheLearnedRoute()
    {
        CamusEndpointPool pool = new("http://a:9007,http://b:9007");
        CamusStatementRouter router = new(
            new Dictionary<string, string>(StringComparer.Ordinal) { ["node-b"] = "http://b:9007" },
            pool, maxHintAgeMs: 60_000);

        router.Learn("db", "s", CamusRouteOpKind.Query, Prefer("node-b", maxAgeMs: 60_000), 0);
        Assert.Equal("http://b:9007", router.SelectEndpoint("db", "s", CamusRouteOpKind.Query, out long observed));

        CamusRoutingAdvice clear = new(1, CamusRoutingDisposition.Clear, null, false, null, 0, "ineligible");
        router.Learn("db", "s", CamusRouteOpKind.Query, clear, observed);

        Assert.Null(router.SelectEndpoint("db", "s", CamusRouteOpKind.Query, out _));
    }

    // ─── Connection-string parsing ────────────────────────────────────────────

    [Fact]
    public void RoutingDefaultsToAuto_InertWithoutATrustMap()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:9101;Database=db");

        // Auto by default, but with no RoutingNodes= there is nothing to route to: the router is
        // null and the connection behaves exactly as Off — the trust map is the real opt-in.
        Assert.Equal(CamusRoutingMode.Auto, builder.RoutingMode);
        Assert.Null(builder.Router);
        Assert.Equal(5_000, builder.RoutingMaxHintAgeMs);
    }

    [Fact]
    public void AutoDefault_EngagesWithATwoNodeTrustMap()
    {
        // No RoutingMode key at all: the map alone turns learning on under the Auto default.
        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:9115,http://localhost:9116;Database=db;" +
            "RoutingNodes='camus-a:7070=http://localhost:9115,camus-b:7070=http://localhost:9116'");

        Assert.NotNull(builder.Router);
    }

    [Fact]
    public void ExplicitOff_DisablesRoutingDespiteATrustMap()
    {
        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:9117,http://localhost:9118;Database=db;RoutingMode=Off;" +
            "RoutingNodes='camus-a:7070=http://localhost:9117,camus-b:7070=http://localhost:9118'");

        Assert.Null(builder.Router);
    }

    [Fact]
    public void LearnedModeBuildsARouterFromTheTrustMap()
    {
        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:9103,http://localhost:9104;Database=db;RoutingMode=Learned;" +
            "RoutingNodes='camus-a:7070=http://localhost:9103,camus-b:7070=http://localhost:9104'");

        Dictionary<string, string> nodes = builder.RoutingNodeAddresses;
        Assert.Equal("http://localhost:9103", nodes["camus-a:7070"]);
        Assert.Equal("http://localhost:9104", nodes["camus-b:7070"]);
        Assert.NotNull(builder.Router);
    }

    [Fact]
    public void TrustMapDropsAddressesOutsideTheEndpointPool()
    {
        // The mapped address must be an operator-listed pool member; anything else is unusable by
        // design — a response can never steer traffic at an address the operator did not configure.
        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:9106;Database=db;RoutingMode=Learned;" +
            "RoutingNodes='camus-a:7070=http://evil:9106,camus-b:7070=http://localhost:9106'");

        Dictionary<string, string> nodes = builder.RoutingNodeAddresses;
        Assert.False(nodes.ContainsKey("camus-a:7070"));
        Assert.Equal("http://localhost:9106", nodes["camus-b:7070"]);
    }

    [Fact]
    public void AutoModeNeedsTwoDistinctMappedEndpoints()
    {
        CamusConnectionStringBuilder oneMapped = new(
            "Endpoint=http://localhost:9108,http://localhost:9109;Database=db;RoutingMode=Auto;" +
            "RoutingNodes='camus-a:7070=http://localhost:9108'");
        Assert.Null(oneMapped.Router);

        CamusConnectionStringBuilder twoMapped = new(
            "Endpoint=http://localhost:9110,http://localhost:9111;Database=db;RoutingMode=Auto;" +
            "RoutingNodes='camus-a:7070=http://localhost:9110,camus-b:7070=http://localhost:9111'");
        Assert.NotNull(twoMapped.Router);
    }

    [Fact]
    public void RoutersAreSharedPerConfiguration()
    {
        // EF rebuilds the builder per DbConnection; a per-builder router would start cold each time.
        const string config =
            "Endpoint=http://localhost:9113,http://localhost:9114;Database=db;RoutingMode=Learned;" +
            "RoutingNodes='camus-a:7070=http://localhost:9113,camus-b:7070=http://localhost:9114'";

        Assert.Same(new CamusConnectionStringBuilder(config).Router, new CamusConnectionStringBuilder(config).Router);
    }

    // ─── REST flow over mocked HTTP ───────────────────────────────────────────

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

    [Fact]
    public async Task LearnedRoutingSteersRepeatedQueriesToTheAdvertisedNode()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("http://localhost:9182/*")
            .RespondWithJson(new { status = "ok", columns = Array.Empty<object>(), rows = Array.Empty<object>(), routing = RoutingJson("camus-b:7070") });

        httpTest
            .ForCallsTo("http://localhost:9184/*")
            .RespondWithJson(new { status = "ok", columns = Array.Empty<object>(), rows = Array.Empty<object>(), routing = RoutingJson("camus-b:7070") });

        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:9182,http://localhost:9184;Database=test;RoutingMode=Learned;" +
            "RoutingNodes='camus-a:7070=http://localhost:9182,camus-b:7070=http://localhost:9184'");
        using CamusConnection connection = new(builder);

        for (int i = 0; i < 3; i++)
        {
            await using CamusCommand command = connection.CreateCamusCommand("SELECT balance FROM accounts WHERE id = @id");
            command.Parameters.Add("@id", ColumnType.Integer64, i);
            await using var reader = await command.ExecuteReaderAsync();
            Assert.NotNull(command.LastRoutingAdvice);
            Assert.Equal(CamusRoutingDisposition.Prefer, command.LastRoutingAdvice!.Disposition);
        }

        // First execution rotates (endpoint A), learns camus-b; every later one goes straight to B.
        httpTest.ShouldHaveCalled("http://localhost:9182/execute-sql-query").Times(1);
        httpTest.ShouldHaveCalled("http://localhost:9184/execute-sql-query").Times(2);
    }

    [Fact]
    public void RequestSerializationCarriesAcceptVersionOnlyWhenNegotiated()
    {
        // The mocked-HTTP tests cannot inspect the posted body (the streaming JSON content is not
        // captured by the test handler), so the request shape is pinned here at the serializer:
        // zero — every pre-routing request — omits the field entirely, and 1 travels verbatim.
        string offQuery = System.Text.Json.JsonSerializer.Serialize(
            new CamusExecuteSqlQueryRequest { Sql = "SELECT 1" },
            CamusJsonSerializerContext.Default.CamusExecuteSqlQueryRequest);
        Assert.DoesNotContain("routingAcceptVersion", offQuery);

        string onQuery = System.Text.Json.JsonSerializer.Serialize(
            new CamusExecuteSqlQueryRequest { Sql = "SELECT 1", RoutingAcceptVersion = 1 },
            CamusJsonSerializerContext.Default.CamusExecuteSqlQueryRequest);
        Assert.Contains("\"routingAcceptVersion\":1", onQuery);

        string offNonQuery = System.Text.Json.JsonSerializer.Serialize(
            new CamusExecuteSqlNonQueryRequest { Sql = "UPDATE t SET a = 1" },
            CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryRequest);
        Assert.DoesNotContain("routingAcceptVersion", offNonQuery);

        string onNonQuery = System.Text.Json.JsonSerializer.Serialize(
            new CamusExecuteSqlNonQueryRequest { Sql = "UPDATE t SET a = 1", RoutingAcceptVersion = 1 },
            CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryRequest);
        Assert.Contains("\"routingAcceptVersion\":1", onNonQuery);
    }

    [Fact]
    public async Task NonQueryAdviceIsLearnedToo()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("http://localhost:9186/*")
            .RespondWithJson(new { status = "ok", rows = 1, routing = RoutingJson("camus-b:7070") });

        httpTest
            .ForCallsTo("http://localhost:9188/*")
            .RespondWithJson(new { status = "ok", rows = 1, routing = RoutingJson("camus-b:7070") });

        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:9186,http://localhost:9188;Database=test;RoutingMode=Learned;" +
            "RoutingNodes='camus-a:7070=http://localhost:9186,camus-b:7070=http://localhost:9188'");
        using CamusConnection connection = new(builder);

        for (int i = 0; i < 3; i++)
        {
            await using CamusCommand command = connection.CreateCamusCommand("UPDATE accounts SET balance = 1 WHERE id = 1");
            Assert.Equal(1, await command.ExecuteNonQueryAsync());
        }

        httpTest.ShouldHaveCalled("http://localhost:9186/execute-sql-non-query").Times(1);
        httpTest.ShouldHaveCalled("http://localhost:9188/execute-sql-non-query").Times(2);
    }

    [Fact]
    public async Task RoutingOffKeepsThePreRoutingRequestShape()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("http://localhost:9190/*")
            .RespondWithJson(new { status = "ok", columns = Array.Empty<object>(), rows = Array.Empty<object>() });

        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:9190;Database=test");
        using CamusConnection connection = new(builder);

        await using CamusCommand command = connection.CreateCamusCommand("SELECT 1");
        await using var reader = await command.ExecuteReaderAsync();

        // The Auto default without a trust map builds no router, so nothing was negotiated or
        // reported; the serializer-level test above pins that an un-negotiated request also omits
        // the field from its JSON entirely — a pre-routing connection string stays byte-identical.
        Assert.Null(builder.Router);
        Assert.Null(command.LastRoutingAdvice);
    }

    [Fact]
    public async Task TransactionPinOverridesEveryLearnedRoute()
    {
        using HttpTest httpTest = new();

        httpTest
            .ForCallsTo("http://localhost:9192/*")
            .RespondWithJson(new { status = "ok", rows = 1, routing = RoutingJson("camus-b:7070") });

        httpTest
            .ForCallsTo("http://localhost:9194/*")
            .RespondWithJson(new { status = "ok", rows = 1 });

        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:9192,http://localhost:9194;Database=test;RoutingMode=Learned;" +
            "RoutingNodes='camus-a:7070=http://localhost:9192,camus-b:7070=http://localhost:9194'");
        using CamusConnection connection = new(builder);

        // Warm the route toward camus-b (9194).
        await using (CamusCommand warm = connection.CreateCamusCommand("UPDATE t SET a = 1"))
            await warm.ExecuteNonQueryAsync();

        // A transaction pinned to 9192 keeps every statement there, advice notwithstanding.
        CamusTransaction transaction = new(10, 20, "http://localhost:9192", connection, builder);
        await using CamusCommand pinned = connection.CreateCamusCommand("UPDATE t SET a = 1");
        pinned.Transaction = transaction;
        Assert.Equal(1, await pinned.ExecuteNonQueryAsync());

        httpTest.ShouldHaveCalled("http://localhost:9192/execute-sql-non-query").Times(2);
    }
}
