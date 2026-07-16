using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusExecuteDDLRequest
{
    [JsonPropertyName("txnIdPT")]
    public long TxnIdPT { get; set; }

    [JsonPropertyName("txnIdCounter")]
    public uint TxnIdCounter { get; set; }

    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonPropertyName("sql")]
    public string? Sql { get; set; }

    /// <summary>Isolation level for the autocommit transaction begun by this DDL request.</summary>
    [JsonPropertyName("isolationLevel")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? IsolationLevel { get; set; }

    /// <summary>Transaction mode for the autocommit transaction begun by this DDL request.</summary>
    [JsonPropertyName("transactionMode")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? TransactionMode { get; set; }

    /// <summary>Locking mode for the autocommit transaction begun by this DDL request.</summary>
    [JsonPropertyName("locking")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Locking { get; set; }
}
