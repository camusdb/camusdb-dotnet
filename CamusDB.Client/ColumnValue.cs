
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// 
/// </summary>
public sealed class ColumnValue
{
    [JsonPropertyName("type")]
    public ColumnType Type { get; set; }

    [JsonPropertyName("strValue")]
    public string? StrValue { get; set; }

    [JsonPropertyName("longValue")]
    public long LongValue { get; set; }

    [JsonPropertyName("floatValue")]
    public double FloatValue { get; set; }

    [JsonPropertyName("boolValue")]
    public bool BoolValue { get; set; }
}
