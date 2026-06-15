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
}
