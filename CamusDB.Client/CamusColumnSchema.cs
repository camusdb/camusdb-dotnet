/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// One entry of a query result's output column schema: the projected column's name and declared
/// <see cref="ColumnType"/>. The server includes an ordered <c>columns</c> array on the
/// <c>execute-sql-query</c> response so the client can report the full result schema (field count,
/// names and types) even when the query matched zero rows — required by ADO/EF consumers that read
/// the reader's schema before any row (notably EF Core's buffered reader under
/// <c>EnableRetryOnFailure</c>).
/// </summary>
public sealed class CamusColumnSchema
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("type")]
    public ColumnType Type { get; set; }
}
