using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusShowBranchesRequest
{
    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }
}
