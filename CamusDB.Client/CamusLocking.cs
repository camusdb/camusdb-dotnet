/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// The concurrency strategy a CamusDB transaction uses to resolve conflicts. Orthogonal to
/// <see cref="CamusIsolationLevel"/>. The enum member names are the exact strings the server accepts in
/// the <c>locking</c> wire field.
/// </summary>
public enum CamusLocking
{
    /// <summary>
    /// Take locks up front; a conflicting transaction blocks (or is aborted by deadlock avoidance) at
    /// lock-acquisition time. The default, and what almost all callers use.
    /// </summary>
    Pessimistic,

    /// <summary>
    /// Take no explicit locks; stage writes and record read observations, and detect write–write and
    /// read–write conflicts only at commit. A losing transaction surfaces as a <see cref="CamusException"/>
    /// from commit and must be retried. Good for low-contention, read-mostly workloads. Non-phantom — use
    /// <see cref="CamusIsolationLevel.Serializable"/> pessimistic when you need phantom protection.
    /// </summary>
    Optimistic,
}
