
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// One backup in the node's catalog, as returned by <see cref="CamusBackupClient"/>.
///
/// <para>A backup is node-wide: every database on a CamusDB server shares one Kahuna node, so an entry
/// here captures all of them at once and is not scoped to the connection's database.</para>
/// </summary>
public sealed class CamusBackupInfo
{
    /// <summary>Catalog id of this backup, as a GUID string. Names the parent of an incremental and the
    /// leaf of a chain.</summary>
    [JsonPropertyName("backupId")]
    public string BackupId { get; set; } = "";

    /// <summary>On-disk manifest format version.</summary>
    [JsonPropertyName("formatVersion")]
    public int FormatVersion { get; set; }

    /// <summary>The backup kind as recorded in the manifest (<c>Full</c>, <c>Incremental</c>, <c>Coordinated</c>).</summary>
    [JsonPropertyName("type")]
    public string Type { get; set; } = "";

    /// <summary>When the backup was published, in UTC.</summary>
    [JsonPropertyName("createdAtUtc")]
    public DateTime CreatedAtUtc { get; set; }

    /// <summary>The backup this one chains onto; null for a full or coordinated root.</summary>
    [JsonPropertyName("parentBackupId")]
    public string? ParentBackupId { get; set; }

    /// <summary>How many partitions the backup covers.</summary>
    [JsonPropertyName("partitionCount")]
    public int PartitionCount { get; set; }

    /// <summary>Node component of the coordinated cut's HLC; non-null only for a coordinated backup.</summary>
    [JsonPropertyName("clusterSnapshotNode")]
    public int? ClusterSnapshotNode { get; set; }

    /// <summary>Physical component of the coordinated cut's HLC; non-null only for a coordinated backup.</summary>
    [JsonPropertyName("clusterSnapshotPhysical")]
    public long? ClusterSnapshotPhysical { get; set; }

    /// <summary>Counter component of the coordinated cut's HLC; non-null only for a coordinated backup.</summary>
    [JsonPropertyName("clusterSnapshotCounter")]
    public uint? ClusterSnapshotCounter { get; set; }

    /// <summary>The kind that was asked for. Differs from <see cref="ActualKind"/> only on a substitution.</summary>
    [JsonPropertyName("requestedKind")]
    public string? RequestedKind { get; set; }

    /// <summary>The kind actually produced.</summary>
    [JsonPropertyName("actualKind")]
    public string? ActualKind { get; set; }

    /// <summary>Why the requested kind was substituted — set when an incremental could no longer be based
    /// on its parent and was taken as a full instead. See <see cref="WasSubstituted"/>.</summary>
    [JsonPropertyName("substitutionReason")]
    public string? SubstitutionReason { get; set; }

    /// <summary>True when this catalog entry's manifest could not be read; only <see cref="BackupId"/> is
    /// meaningful, and <see cref="InvalidReason"/> says why.</summary>
    [JsonPropertyName("isInvalid")]
    public bool IsInvalid { get; set; }

    /// <summary>Why the manifest could not be read; null when <see cref="IsInvalid"/> is false.</summary>
    [JsonPropertyName("invalidReason")]
    public string? InvalidReason { get; set; }

    /// <summary>Earliest restorable point of a resolved chain, in Unix epoch milliseconds. Set only on the
    /// <b>first</b> entry (the root) of a list returned by <see cref="CamusBackupClient.GetChainAsync"/>,
    /// which is where the server reports the whole chain's coverage; null everywhere else, including on
    /// every entry of a plain <see cref="CamusBackupClient.ListBackupsAsync"/>.</summary>
    [JsonPropertyName("minRecoverablePhysicalMs")]
    public long? MinRecoverablePhysicalMs { get; set; }

    /// <summary>Latest restorable point of a resolved chain, in Unix epoch milliseconds. Set alongside
    /// <see cref="MinRecoverablePhysicalMs"/> on the first entry of a resolved chain.</summary>
    [JsonPropertyName("maxRecoverablePhysicalMs")]
    public long? MaxRecoverablePhysicalMs { get; set; }

    /// <summary>Cluster identity the backup was taken under; a restore refuses to chain artifacts from a
    /// different cluster. Null on a standalone or pre-cluster backup.</summary>
    [JsonPropertyName("clusterId")]
    public string? ClusterId { get; set; }

    /// <summary>The node that coordinated the backup. Null on a standalone or pre-cluster backup.</summary>
    [JsonPropertyName("coordinatorNode")]
    public string? CoordinatorNode { get; set; }

    /// <summary>
    /// Whether the server produced a different kind than was requested — an incremental whose parent had
    /// aged past the retention floor is transparently taken as a full instead. Worth surfacing in an
    /// operator log: the backup succeeded, but it cost a full image rather than an increment.
    /// </summary>
    [JsonIgnore]
    public bool WasSubstituted
        => RequestedKind is not null && ActualKind is not null && !string.Equals(RequestedKind, ActualKind, StringComparison.Ordinal);
}
