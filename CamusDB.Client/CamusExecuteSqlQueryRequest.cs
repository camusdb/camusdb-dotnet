
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

public sealed class CamusExecuteSqlQueryRequest
{
    [JsonPropertyName("txnIdPT")]
    public long TxnIdPT { get; set; }

    [JsonPropertyName("txnIdCounter")]
    public uint TxnIdCounter { get; set; }

    [JsonPropertyName("databaseName")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? DatabaseName { get; set; }

    [JsonPropertyName("sql")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Sql { get; set; }

    [JsonPropertyName("parameters")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, ColumnValue>? Parameters { get; set; }

    /// <summary>
    /// Handle from <c>/prepare-sql-statement</c>. When set, <see cref="Sql"/>, <see cref="DatabaseName"/>
    /// and <see cref="Parameters"/> must be absent — the handle already names all three — and
    /// <see cref="PositionalParameters"/> carries the values.
    /// </summary>
    [JsonPropertyName("statementId")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? StatementId { get; set; }

    /// <summary>Values for a prepared execution, in the binding order the prepare reply published.</summary>
    [JsonPropertyName("positionalParameters")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public List<ColumnValue>? PositionalParameters { get; set; }
}

