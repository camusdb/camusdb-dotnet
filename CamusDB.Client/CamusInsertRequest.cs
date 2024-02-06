
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Newtonsoft.Json;

namespace CamusDB.Client;

public sealed class CamusInsertRequest
{
    [JsonProperty("txnIdPT")]
    public long TxnIdPT { get; set; }

    [JsonProperty("txnIdCounter")]
    public uint TxnIdCounter { get; set; }

    [JsonProperty("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonProperty("tableName")]
    public string? TableName { get; set; }

    [JsonProperty("values")]
    public Dictionary<string, ColumnValue>? Values { get; set; }
}

