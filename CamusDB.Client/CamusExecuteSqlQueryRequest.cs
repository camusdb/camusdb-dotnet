
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Newtonsoft.Json;

namespace CamusDB.Client;

public sealed class CamusExecuteSqlQueryRequest
{
    [JsonProperty("txnIdPT")]
    public long TxnIdPT { get; set; }

    [JsonProperty("txnIdCounter")]
    public uint TxnIdCounter { get; set; }

    [JsonProperty("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonProperty("sql")]
    public string? Sql { get; set; }

    [JsonProperty("parameters")]
    public Dictionary<string, ColumnValue>? Parameters { get; set; }
}

