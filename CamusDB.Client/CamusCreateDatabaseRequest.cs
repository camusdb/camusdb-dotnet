using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusCreateDatabaseRequest
{
    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonPropertyName("ifNotExists")]
    public bool IfNotExists { get; set; }
}
