using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusStartTransactionRequest
{
    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }
}
