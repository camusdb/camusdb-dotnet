
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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
