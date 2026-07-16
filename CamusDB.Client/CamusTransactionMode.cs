/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// Whether a CamusDB transaction may write. The enum member names are the exact strings the server
/// accepts in the <c>transactionMode</c> wire field.
/// </summary>
public enum CamusTransactionMode
{
    /// <summary>The transaction may read and write (the default).</summary>
    ReadWrite,

    /// <summary>
    /// The transaction only reads. Combined with <see cref="CamusIsolationLevel.Serializable"/> this is a
    /// lock-free consistent snapshot pinned to the instant it began, resumable across several requests.
    /// </summary>
    ReadOnly,
}
