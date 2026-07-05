using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusShowAncestorsRequest
{
    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }
}
