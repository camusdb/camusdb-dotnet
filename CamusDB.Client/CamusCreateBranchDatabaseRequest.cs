using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusCreateBranchDatabaseRequest
{
    [JsonPropertyName("branchName")]
    public string? BranchName { get; set; }

    [JsonPropertyName("sourceDatabaseName")]
    public string? SourceDatabaseName { get; set; }

    [JsonPropertyName("ifNotExists")]
    public bool IfNotExists { get; set; }
}
