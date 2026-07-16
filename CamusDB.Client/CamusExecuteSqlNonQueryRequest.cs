
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

public sealed class CamusExecuteSqlNonQueryRequest
{
    [JsonPropertyName("txnIdPT")]
    public long TxnIdPT { get; set; }

    [JsonPropertyName("txnIdCounter")]
    public uint TxnIdCounter { get; set; }

    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonPropertyName("sql")]
    public string? Sql { get; set; }

    [JsonPropertyName("parameters")]
    public Dictionary<string, ColumnValue>? Parameters { get; set; }

    /// <summary>Isolation level for the autocommit transaction begun by this request. Ignored when it
    /// resumes an existing transaction (a non-zero <see cref="TxnIdPT"/>).</summary>
    [JsonPropertyName("isolationLevel")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? IsolationLevel { get; set; }

    /// <summary>Transaction mode for the autocommit transaction begun by this request.</summary>
    [JsonPropertyName("transactionMode")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? TransactionMode { get; set; }

    /// <summary>Locking mode for the autocommit transaction begun by this request.</summary>
    [JsonPropertyName("locking")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Locking { get; set; }
}

