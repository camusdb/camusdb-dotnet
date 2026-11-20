/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// The answer of <c>/commit-transaction</c> and <c>/rollback-transaction</c>. Only a commit answer
/// carries <see cref="CommitTimestamp"/>.
/// </summary>
internal sealed class CamusCommitTransactionResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    /// <summary>The commit timestamp of the transaction. The server sends null when the transaction wrote
    /// nothing, and an older server does not send the field.</summary>
    [JsonPropertyName("commitTimestamp")]
    public CamusHlcTimestamp? CommitTimestamp { get; set; }
}
