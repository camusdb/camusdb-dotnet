
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// What a backup retention pass reclaimed — or, for a preview, would reclaim. Retention deletes only
/// whole chains, always leaving a valid full root for every retained leaf, so a deletion list never
/// leaves an unrestorable leaf behind.
/// </summary>
public sealed class CamusBackupGcResult
{
    /// <summary>False for a dry-run preview; true when the pass was actually applied.</summary>
    [JsonPropertyName("applied")]
    public bool Applied { get; set; }

    /// <summary>Bytes reclaimed, or for a preview the bytes that would be.</summary>
    [JsonPropertyName("bytesReclaimed")]
    public long BytesReclaimed { get; set; }

    /// <summary>Catalogued backups dropped by the retention policy.</summary>
    [JsonPropertyName("retentionDeletions")]
    public List<CamusBackupGcDeletion>? RetentionDeletions { get; set; }

    /// <summary>Files and directories under the backup root that belong to no catalogued backup.</summary>
    [JsonPropertyName("orphanReclamations")]
    public List<CamusBackupGcOrphan>? OrphanReclamations { get; set; }
}

/// <summary>One backup dropped (or, in a preview, selected to be dropped) by the retention policy.</summary>
public sealed class CamusBackupGcDeletion
{
    /// <summary>Catalog id of the deleted backup.</summary>
    [JsonPropertyName("backupId")]
    public string BackupId { get; set; } = "";

    /// <summary>The deleted backup's kind.</summary>
    [JsonPropertyName("type")]
    public string Type { get; set; } = "";

    /// <summary>When the deleted backup was originally published, in UTC.</summary>
    [JsonPropertyName("createdAtUtc")]
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>Bytes this backup accounted for.</summary>
    [JsonPropertyName("bytes")]
    public long Bytes { get; set; }

    /// <summary>Which retention limit selected it (chain count, age, or total bytes).</summary>
    [JsonPropertyName("reason")]
    public string Reason { get; set; } = "";
}

/// <summary>One stray filesystem entry under the backup root that belongs to no catalogued backup.</summary>
public sealed class CamusBackupGcOrphan
{
    /// <summary>Name of the orphaned entry, relative to the backup root.</summary>
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    /// <summary>True when the entry is a directory rather than a file.</summary>
    [JsonPropertyName("isDirectory")]
    public bool IsDirectory { get; set; }

    /// <summary>Why the entry was classified as an orphan.</summary>
    [JsonPropertyName("reason")]
    public string Reason { get; set; } = "";
}
