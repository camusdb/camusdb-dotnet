using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusDropDatabaseRequest
{
    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }
}
