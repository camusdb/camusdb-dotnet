
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// Body of <c>/prepare-sql-statement</c>: the database and SQL the returned handle stands for. Both are
/// captured by the server once and reused by every execution of that handle, so neither travels again.
/// </summary>
public sealed class CamusPrepareStatementRequest
{
    [JsonPropertyName("databaseName")]
    public string? DatabaseName { get; set; }

    [JsonPropertyName("sql")]
    public string? Sql { get; set; }
}

/// <summary>
/// Reply to <c>/prepare-sql-statement</c>. <see cref="ParameterNames"/> is the binding order every later
/// execution must follow: the value sent at index <c>i</c> binds to the name at index <c>i</c>. Names keep
/// their leading <c>@</c>, which is what lets this client keep binding by name in its own API and map onto
/// ordinals just before the write.
/// </summary>
internal sealed class CamusPrepareStatementResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    /// <summary>Opaque handle, valid on the node that minted it and for the principal that prepared it.</summary>
    [JsonPropertyName("statementId")]
    public string? StatementId { get; set; }

    [JsonPropertyName("parameterNames")]
    public List<string>? ParameterNames { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}

/// <summary>Body of <c>/close-sql-statement</c>. Closing an unknown or already-closed handle succeeds.</summary>
public sealed class CamusCloseStatementRequest
{
    [JsonPropertyName("statementId")]
    public string? StatementId { get; set; }
}

/// <summary>Reply to <c>/close-sql-statement</c>.</summary>
internal sealed class CamusCloseStatementResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }
}
