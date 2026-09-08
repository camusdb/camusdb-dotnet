/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace CamusDB.Client;

/// <summary>How a connection uses learned statement routing. Parsed from the <c>RoutingMode=</c> key.</summary>
public enum CamusRoutingMode
{
    /// <summary>Rotate over the configured endpoints; never negotiate or learn.</summary>
    Off = 0,

    /// <summary>Negotiate routing metadata and prefer learned destinations for unpinned statements.</summary>
    Learned = 1,

    /// <summary>
    /// Behave as <see cref="Learned"/> when <c>RoutingNodes=</c> maps at least two distinct
    /// configured endpoints, else as <see cref="Off"/> — one destination is not a routing
    /// decision, and a single load-balancer URL is not a set of routable database nodes.
    /// The default: a trust map is the opt-in, not a second switch.
    /// </summary>
    Auto = 2,
}

/// <summary>
/// Learned statement routing for one deployment: the operator-configured trust map from server
/// node identities to <c>Endpoint=</c> pool members, plus the bounded cache of learned routes.
///
/// <para><b>Shared per deployment, like the endpoint pool.</b> EF Core rebuilds the
/// connection-string builder for every <see cref="System.Data.Common.DbConnection"/>, so
/// builder-owned routing state would start cold on every request. Routers are therefore shared
/// process-wide, keyed by the deployment plus the routing configuration —
/// see <see cref="Shared"/> and the same reasoning on <see cref="CamusEndpointPool"/>.</para>
///
/// <para><b>The trust map is the routing authority.</b> Advice names an opaque node identity; it
/// becomes a destination only through this map, and only when the mapped address is a member of
/// the <c>Endpoint=</c> pool — so a response can never steer traffic (or credentials) anywhere
/// the operator did not list, and every routable address already passed the same TLS validation
/// as the pool itself.</para>
///
/// <para><b>Selection is a bias, never an authority.</b> A learned route that is missing, expired,
/// unmapped, or pointing at a quarantined endpoint simply falls back to the pool's ordinary
/// rotation, and an explicit transaction's endpoint pin always wins — the caller consults this
/// router only for unpinned statements. Routing adds no retry of any kind.</para>
/// </summary>
internal sealed class CamusStatementRouter
{
    // Routers keyed by deployment + routing configuration — see Shared().
    private static readonly ConcurrentDictionary<string, CamusStatementRouter> SharedRouters = new(StringComparer.Ordinal);

    /// <summary>How many distinct routing configurations the process shares a router for.</summary>
    private const int MaxSharedRouters = 1024;

    /// <summary>Bound on learned route entries; high-cardinality SQL must not grow without limit.</summary>
    private const int RouteCacheMaxEntries = 4_096;

    /// <summary>Bound on the bytes the route cache retains, keys included.</summary>
    private const long RouteCacheMaxBytes = 4 * 1024 * 1024;

    /// <summary>
    /// The process-wide router for one routing configuration, created on first use. Past the cap a
    /// caller gets a private router — its own learned state, which is what every caller had before
    /// sharing existed.
    /// </summary>
    public static CamusStatementRouter Shared(string routerKey, Func<CamusStatementRouter> factory)
    {
        if (SharedRouters.TryGetValue(routerKey, out CamusStatementRouter? existing))
            return existing;

        if (SharedRouters.Count >= MaxSharedRouters)
            return factory();

        return SharedRouters.GetOrAdd(routerKey, _ => factory());
    }

    private readonly Dictionary<string, string> nodeAddresses;

    private readonly CamusEndpointPool pool;

    private readonly CamusStatementRouteCache routes = new(RouteCacheMaxEntries, RouteCacheMaxBytes);

    private readonly long maxHintAgeMs;

    /// <summary>
    /// Monotonic clock in milliseconds. A test seam: TTL expiry is time-based, and a controllable
    /// clock is the difference between testing it and sleeping in tests.
    /// </summary>
    internal Func<long> Clock { get; set; } = static () => Environment.TickCount64;

    /// <param name="nodeAddresses">
    /// Node identity → endpoint address; every value must already be a member of
    /// <paramref name="pool"/> (the builder filters non-members out before construction).
    /// </param>
    public CamusStatementRouter(Dictionary<string, string> nodeAddresses, CamusEndpointPool pool, long maxHintAgeMs)
    {
        this.nodeAddresses = nodeAddresses;
        this.pool = pool;
        this.maxHintAgeMs = Math.Max(1, maxHintAgeMs);
    }

    /// <summary>Learned-route entry count, for tests and diagnostics.</summary>
    internal int LearnedRouteCount => routes.Count;

    /// <summary>
    /// The learned endpoint for one unpinned statement, or null to use the pool's rotation. Also
    /// captures the route entry's revision so the reply's advice can be applied conditionally —
    /// a late reply must never overwrite a route a faster reply already refreshed.
    /// </summary>
    public string? SelectEndpoint(string database, string sql, CamusRouteOpKind kind, out long observedRevision)
    {
        CamusStatementRouteKey key = new(database, sql, kind);
        long now = Clock();

        string? nodeId = routes.TryGet(key, now, out observedRevision);
        if (nodeId is null || !nodeAddresses.TryGetValue(nodeId, out string? endpoint))
            return null;

        // A learned endpoint sitting in quarantine is skipped rather than insisted on: quarantine
        // means the node stopped answering, and routing is a latency bias, not an availability bet.
        return pool.IsQuarantined(endpoint) ? null : endpoint;
    }

    /// <summary>
    /// Applies a successful reply's advice. Ignored unless it is version 1, well-formed, and — for
    /// a prefer — parameter-independent in scope, positive in TTL, and naming a mapped identity.
    /// Every rejection is a plain early return, so learning can never fail the statement.
    /// </summary>
    public void Learn(string database, string sql, CamusRouteOpKind kind, CamusRoutingAdvice? advice, long observedRevision)
    {
        if (advice is null || advice.Version != CamusRoutingAdvice.AcceptVersion)
            return;

        CamusStatementRouteKey key = new(database, sql, kind);

        if (advice.Disposition == CamusRoutingDisposition.Clear)
        {
            routes.Clear(key, observedRevision);
            return;
        }

        if (advice.Disposition != CamusRoutingDisposition.Prefer
            || !advice.ParametersIndependentScope
            || advice.PreferredNodeId is null
            || advice.MaxAgeMs <= 0
            || !nodeAddresses.ContainsKey(advice.PreferredNodeId))
            return;

        long now = Clock();
        long ttl = Math.Min(advice.MaxAgeMs, maxHintAgeMs);
        routes.Learn(key, advice.PreferredNodeId, advice.DependencyToken, now + ttl, observedRevision, now);
    }
}
