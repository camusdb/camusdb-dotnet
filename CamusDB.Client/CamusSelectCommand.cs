﻿
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Newtonsoft.Json;

namespace CamusDB.Client;

internal sealed class CamusExecuteSqlQueryResponse
{
    [JsonProperty("status")]
    public string? Status { get; set; }

    [JsonProperty("rows")]
    public List<Dictionary<string, ColumnValue>>? Rows { get; set; }
}