using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusStartTransactionRequest
{
    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonPropertyName("isolationLevel")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? IsolationLevel { get; set; }

    [JsonPropertyName("transactionMode")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? TransactionMode { get; set; }

    [JsonPropertyName("locking")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Locking { get; set; }
}
