using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusExecuteDDLRequest
{
    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonPropertyName("sql")]
    public string? Sql { get; set; }
}
