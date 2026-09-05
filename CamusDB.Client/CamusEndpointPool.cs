using System.Collections.Concurrent;

namespace CamusDB.Client;

/// <summary>
/// The endpoints one deployment's connections rotate through, and which of them are currently set aside
/// after failing to answer at all.
///
/// <para><b>Why one pool per deployment rather than per builder.</b> Entity Framework builds a
/// <see cref="CamusConnectionStringBuilder"/> for every <see cref="System.Data.Common.DbConnection"/> it
/// opens, so a builder-owned pool starts its rotation at the first endpoint on every request — a
/// multi-endpoint deployment would send essentially all of its traffic to one node — and forgets, just as
/// often, that another node was unreachable a moment ago. Neither the rotation nor the health it depends
/// on is a property of one short-lived builder.</para>
///
/// <para><b>Why quarantine expires.</b> A pool that outlives the request that marked an endpoint cannot
/// mark it forever: with a single endpoint configured — the common case — the first refused connection
/// would otherwise end the process's ability to reach the database at all, long after the node came back.
/// An endpoint is therefore set aside for <see cref="QuarantinePeriod"/> and then drawn again; a node that
/// is still down is simply re-marked by the request that draws it, at a cost of one failed request per
/// period.</para>
/// </summary>
internal sealed class CamusEndpointPool
{
    /// <summary>How long an endpoint that failed at the transport level is passed over before being tried
    /// again.</summary>
    public static readonly TimeSpan QuarantinePeriod = TimeSpan.FromSeconds(30);

    // Pools keyed by the endpoint list they serve — see Shared().
    private static readonly ConcurrentDictionary<string, CamusEndpointPool> SharedPools = new(StringComparer.Ordinal);

    /// <summary>The process-wide pool for an <c>Endpoint=</c> value, created on first use. Keyed by the
    /// endpoint list alone, so connection strings differing only in database, credentials or timeouts
    /// share one rotation and one view of which nodes are answering.</summary>
    public static CamusEndpointPool Shared(string endpointConfig)
    {
        if (SharedPools.TryGetValue(endpointConfig, out CamusEndpointPool? existing))
            return existing;

        // Past the cap a caller gets its own pool rather than growing a dictionary nothing empties: it
        // keeps its own rotation and health view, which is what every pool did before sharing existed.
        if (SharedPools.Count >= MaxSharedPools)
            return new CamusEndpointPool(endpointConfig);

        return SharedPools.GetOrAdd(endpointConfig, config => new CamusEndpointPool(config));
    }

    /// <summary>How many distinct endpoint lists the process shares a pool for.</summary>
    private const int MaxSharedPools = 1024;

    private readonly string[] endpoints;

    /// <summary><see cref="Environment.TickCount64"/> deadlines, indexed alongside
    /// <see cref="endpoints"/>. Zero — the initial value — is in the past, so an endpoint that has never
    /// failed needs no special case.</summary>
    private readonly long[] quarantinedUntil;

    private readonly object sync = new();

    private int nextEndpointIndex;

    public CamusEndpointPool(string endpointConfig)
    {
        endpoints = endpointConfig
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        quarantinedUntil = new long[endpoints.Length];
    }

    /// <summary>
    /// The next endpoint to send to, skipping those in quarantine.
    ///
    /// <para>When every endpoint is quarantined the one closest to leaving is returned rather than an
    /// error: the deployment is evidently down, and letting the request go and fail against the real node
    /// reports why, where a synthetic "no endpoints" would replace the server's diagnosis with the
    /// driver's bookkeeping.</para>
    /// </summary>
    public string GetNextEndpoint()
    {
        lock (sync)
        {
            long now = Environment.TickCount64;
            int fallbackIndex = -1;

            for (int i = 0; i < endpoints.Length; i++)
            {
                int endpointIndex = nextEndpointIndex;
                nextEndpointIndex = (nextEndpointIndex + 1) % endpoints.Length;

                if (quarantinedUntil[endpointIndex] <= now)
                    return endpoints[endpointIndex];

                if (fallbackIndex < 0 || quarantinedUntil[endpointIndex] < quarantinedUntil[fallbackIndex])
                    fallbackIndex = endpointIndex;
            }

            if (fallbackIndex >= 0)
                return endpoints[fallbackIndex];
        }

        throw new CamusException("CADB0000", "No reachable CamusDB endpoints are available");
    }

    /// <summary>Sets an endpoint aside for <see cref="QuarantinePeriod"/>, so the endpoints that are
    /// answering carry the traffic meanwhile.</summary>
    public void MarkUnreachable(string endpoint)
    {
        lock (sync)
        {
            for (int i = 0; i < endpoints.Length; i++)
            {
                if (string.Equals(endpoints[i], endpoint, StringComparison.OrdinalIgnoreCase))
                {
                    quarantinedUntil[i] = Environment.TickCount64 + (long)QuarantinePeriod.TotalMilliseconds;
                    return;
                }
            }
        }
    }
}
