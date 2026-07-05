using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// One row returned by <see cref="CamusConnection.ShowBranchesAsync"/> or
/// <see cref="CamusConnection.ShowAncestorsAsync"/>.
/// </summary>
public sealed class CamusBranchRow
{
    /// <summary>User-visible name of the database.</summary>
    [JsonPropertyName("database")]
    public string? Database { get; set; }

    /// <summary>Stable opaque id; never changes, never reused.</summary>
    [JsonPropertyName("id")]
    public string? Id { get; set; }

    /// <summary>
    /// Distance from the query root.
    /// For SHOW BRANCHES: 1 = direct child, 2 = grandchild, …
    /// For SHOW ANCESTORS: 1 = immediate parent, 2 = grandparent, …
    /// </summary>
    [JsonPropertyName("depth")]
    public int Depth { get; set; }

    /// <summary>Immediate-parent name. Present in SHOW BRANCHES results; null in SHOW ANCESTORS.</summary>
    [JsonPropertyName("parent")]
    public string? Parent { get; set; }

    /// <summary>HLC timestamp string of the fork that created this branch from its parent.</summary>
    [JsonPropertyName("forkTimestamp")]
    public string? ForkTimestamp { get; set; }
}
