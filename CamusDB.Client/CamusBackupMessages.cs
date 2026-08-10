
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>Body for <c>POST /v1/backups/incremental</c>. The full and coordinated endpoints take none.</summary>
internal sealed class CamusTakeBackupRequest
{
    [JsonPropertyName("parentBackupId")]
    public string? ParentBackupId { get; set; }
}

/// <summary>Envelope for taking a single backup.</summary>
internal sealed class CamusBackupResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("backup")]
    public CamusBackupInfo? Backup { get; set; }
}

/// <summary>Envelope for listing the catalog or resolving a chain. A chain is ordered root-first.</summary>
internal sealed class CamusBackupListResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("backups")]
    public List<CamusBackupInfo>? Backups { get; set; }
}

/// <summary>Envelope for a retention run or preview.</summary>
internal sealed class CamusBackupGcResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("applied")]
    public bool Applied { get; set; }

    [JsonPropertyName("bytesReclaimed")]
    public long BytesReclaimed { get; set; }

    [JsonPropertyName("retentionDeletions")]
    public List<CamusBackupGcDeletion>? RetentionDeletions { get; set; }

    [JsonPropertyName("orphanReclamations")]
    public List<CamusBackupGcOrphan>? OrphanReclamations { get; set; }
}
