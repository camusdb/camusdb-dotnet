
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Text.Json.Serialization;

namespace CamusDB.Client;

internal sealed class CamusExecuteSqlNonQueryResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("rows")]
    public int Rows { get; set; }

    /// <summary>Advisory routing metadata — absent unless the request negotiated it
    /// (<c>routingAcceptVersion = 1</c>) and the statement produced advice.</summary>
    [JsonPropertyName("routing")]
    public CamusRoutingMetadataResponse? Routing { get; set; }

    /// <summary>The output columns of an <c>INSERT … RETURNING</c>. Absent for a statement without
    /// RETURNING and for a request that set <c>discardReturningRows</c>.</summary>
    [JsonPropertyName("columns")]
    public JsonElement? Columns { get; set; }

    /// <summary>The RETURNING rows, positional against <see cref="Columns"/>: the same encoding as the
    /// <c>rows</c> of a query response. Present when <see cref="Columns"/> is.</summary>
    [JsonPropertyName("returningRows")]
    public JsonElement? ReturningRows { get; set; }
}