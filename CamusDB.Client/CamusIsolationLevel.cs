/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// The SQL isolation level a CamusDB transaction runs at. Orthogonal to <see cref="CamusLocking"/>.
/// The enum member names are the exact strings the server accepts in the <c>isolationLevel</c> wire field.
/// </summary>
public enum CamusIsolationLevel
{
    /// <summary>Lock-free reads of the latest committed value; no repeatable-read / phantom protection.</summary>
    ReadCommitted,

    /// <summary>
    /// Strongest level — the outcome is equivalent to some serial order. A read-write transaction takes
    /// point/range locks; a read-only one is a lock-free consistent snapshot (combine with
    /// <see cref="CamusTransactionMode.ReadOnly"/>).
    /// </summary>
    Serializable,
}
