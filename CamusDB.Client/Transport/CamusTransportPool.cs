
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace CamusDB.Client.Transport;

/// <summary>
/// The transports in use, one per deployment-and-identity rather than one per
/// <see cref="CamusConnectionStringBuilder"/>.
///
/// <para><b>Why this exists.</b> A transport owns server-side state that outlives the request that
/// created it — prepared-statement registrations above all, and for gRPC a pool of long-lived
/// <c>BatchExecute</c> streams and their channels. Entity Framework builds a fresh builder for every
/// <see cref="System.Data.Common.DbConnection"/> it opens, so a builder-owned transport meant a fresh
/// registration cache per <c>DbContext</c>: the auto-prepare policy, which is shared process-wide and
/// therefore reports a hot statement as already prepared, would hand each new transport a statement it
/// had never registered, and each one would register it again. Nothing closed the previous ones —
/// <see cref="CamusConnection.Close"/> does not, and the eviction that does is driven by the shared
/// policy, which never grows past its own cap. Registrations therefore accumulated on the server as
/// <c>hot statements × DbContext instances</c> until the per-principal cap refused them (<c>CADB0521</c>),
/// at which point every statement quietly fell back to inline execution.</para>
///
/// <para>Sharing puts the registration cache on the same lifetime as the decision to prepare, which is
/// what the policy always assumed: one registration per statement per deployment, evicted by the policy's
/// LRU exactly as its cap describes.</para>
///
/// <para><b>Lifetime.</b> A shared transport lives as long as the process, like the token provider and
/// endpoint pool it is keyed beside. That is not a leak the builder-owned transports avoided — those were
/// merely dropped without being closed — and it is bounded by the number of distinct deployments and
/// identities a process talks to, not by how many connections it opens.</para>
/// </summary>
internal static class CamusTransportPool
{
    private static readonly ConcurrentDictionary<string, ICamusTransport> Transports = new(StringComparer.Ordinal);

    /// <summary>
    /// How many distinct deployment-and-identity transports the process shares. Past this, a caller gets
    /// an unshared transport rather than growing a dictionary nothing empties. Sharing is an optimization
    /// — an unshared transport is correct, it simply registers its own prepared statements — so the cap
    /// trades that optimization for a bound on a process that connects as unboundedly many identities.
    /// </summary>
    private const int MaxTransports = 1024;

    /// <summary>The transport for <paramref name="key"/>, created once per process.</summary>
    public static ICamusTransport Shared(string key, Func<ICamusTransport> factory)
    {
        if (Transports.TryGetValue(key, out ICamusTransport? existing))
            return existing;

        if (Transports.Count >= MaxTransports)
            return factory();

        return Transports.GetOrAdd(key, _ => factory());
    }
}
