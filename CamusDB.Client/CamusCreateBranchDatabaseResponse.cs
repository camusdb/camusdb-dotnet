using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusCreateBranchDatabaseResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}
