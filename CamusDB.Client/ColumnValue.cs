
using Newtonsoft.Json;

namespace CamusDB.Client;

/// <summary>
/// 
/// </summary>
public sealed class ColumnValue
{
    [JsonProperty("type")]
    public ColumnType Type { get; set; }

    [JsonProperty("value")]
    public string? Value { get; set; }
}
