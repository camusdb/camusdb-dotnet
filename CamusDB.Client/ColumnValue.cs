
using Newtonsoft.Json;

namespace CamusDB.Client;

/// <summary>
/// 
/// </summary>
public sealed class ColumnValue
{
    [JsonProperty("type")]
    public ColumnType Type { get; set; }

    [JsonProperty("strValue")]
    public string? StrValue { get; set; }

    [JsonProperty("longValue")]
    public long LongValue { get; set; }

    [JsonProperty("boolValue")]
    public bool BoolValue { get; set; }
}
