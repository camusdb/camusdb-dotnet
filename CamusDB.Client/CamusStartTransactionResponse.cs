
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Newtonsoft.Json;

namespace CamusDB.Client;

public sealed class CamusStartTransactionResponse
{
    [JsonProperty("txnIdPT")]
    public long TxnIdPT { get; set; }

    [JsonProperty("txnIdCounter")]
    public uint TxnIdCounter { get; set; }

    [JsonProperty("status")]
    public string? Status { get; set; }
}
