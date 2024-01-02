
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Data.Common;
using Flurl.Http;
using Newtonsoft.Json;

namespace CamusDB.Client;

public class ExecuteSqlQueryResponse
{
    [JsonProperty("status")]
    public string? Status { get; set; }

    [JsonProperty("rows")]
    public List<Dictionary<string, ColumnValue>>? Rows { get; set; }
}