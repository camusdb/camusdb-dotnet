/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// One node's disk, CPU and memory, as returned by <see cref="CamusConnection.GetNodeStatsAsync(CancellationToken)"/>
/// (the server's gRPC <c>GetNodeStats</c> call, or <c>GET /v1/admin/stats</c> over REST — both carry the
/// same fields, so this model is transport-agnostic).
///
/// <para><b>The -1 rule.</b> Every size is in bytes. A value the node could not read is <c>-1</c>, never
/// <c>0</c>, and <see cref="Errors"/> names the field and the reason. A value the server did not send
/// (an older server, a field added later) also reads as <c>-1</c>.</para>
///
/// <para><b>One node.</b> The answer is for the node that received the call — <see cref="Address"/> — not
/// for the cluster. Monitoring a cluster means one call per configured endpoint
/// (<see cref="CamusConnection.Endpoints"/>).</para>
///
/// <para>With authentication on, the account needs the superuser attribute or the <c>MONITOR</c>
/// privilege (<c>GRANT MONITOR ON *.* TO &lt;user&gt;</c>); anything else fails with <c>CADB0517</c>.</para>
/// </summary>
public sealed class CamusNodeStats
{
    /// <summary>The configured endpoint this answer came from. Set by the driver, not sent by the server.</summary>
    [JsonIgnore]
    public string Address { get; set; } = "";

    [JsonPropertyName("node")]
    public CamusNodeIdentity Node { get; set; } = new();

    /// <summary>The data volume first (<see cref="CamusDiskVolumeStats.Role"/> <c>data</c>); a <c>wal</c> entry follows only when the WAL is on its own volume.</summary>
    [JsonPropertyName("disks")]
    public List<CamusDiskVolumeStats> Disks { get; set; } = [];

    [JsonPropertyName("cpu")]
    public CamusCpuStats Cpu { get; set; } = new();

    [JsonPropertyName("memory")]
    public CamusMemoryStats Memory { get; set; } = new();

    /// <summary>One entry per value reported as -1.</summary>
    [JsonPropertyName("errors")]
    public List<CamusNodeStatsError> Errors { get; set; } = [];

    /// <summary>The volume that holds the node's data directory — the one the write gate watches — or null when the server sent none.</summary>
    [JsonIgnore]
    public CamusDiskVolumeStats? DataVolume => Disks.Find(static d => d.Role == CamusDiskVolumeStats.DataRole);
}

/// <summary>Which node answered, and when.</summary>
public sealed class CamusNodeIdentity
{
    /// <summary>The node's Raft node id; empty early in boot.</summary>
    [JsonPropertyName("nodeId")]
    public string NodeId { get; set; } = "";

    /// <summary>The node's Raft endpoint (<c>host:port</c>); empty early in boot.</summary>
    [JsonPropertyName("endpoint")]
    public string Endpoint { get; set; } = "";

    [JsonPropertyName("version")]
    public string Version { get; set; } = "";

    [JsonPropertyName("clusterMode")]
    public bool ClusterMode { get; set; }

    [JsonPropertyName("uptimeSeconds")]
    public long UptimeSeconds { get; set; } = -1;

    /// <summary>When the server built the answer, Unix epoch milliseconds (UTC).</summary>
    [JsonPropertyName("sampledAtUnixMs")]
    public long SampledAtUnixMs { get; set; } = -1;
}

/// <summary>One volume the engine writes to.</summary>
public sealed class CamusDiskVolumeStats
{
    /// <summary>The <see cref="Role"/> of the volume that holds the data directory.</summary>
    public const string DataRole = "data";

    /// <summary>The <see cref="Role"/> of a Raft WAL directory on a volume of its own.</summary>
    public const string WalRole = "wal";

    [JsonPropertyName("role")]
    public string Role { get; set; } = "";

    [JsonPropertyName("path")]
    public string Path { get; set; } = "";

    [JsonPropertyName("mountPoint")]
    public string MountPoint { get; set; } = "";

    [JsonPropertyName("totalBytes")]
    public long TotalBytes { get; set; } = -1;

    [JsonPropertyName("freeBytes")]
    public long FreeBytes { get; set; } = -1;

    /// <summary>Free space the server may still write; the value the watermark compares.</summary>
    [JsonPropertyName("availableBytes")]
    public long AvailableBytes { get; set; } = -1;

    [JsonPropertyName("usedBytes")]
    public long UsedBytes { get; set; } = -1;

    /// <summary>The write-admission watermark (<c>min_free_disk_bytes</c>); 0 on a <c>wal</c> entry, which the gate does not watch.</summary>
    [JsonPropertyName("minFreeDiskBytes")]
    public long MinFreeDiskBytes { get; set; } = -1;

    /// <summary>True while the node refuses writes because this volume is too full (<c>CADB0536</c>).</summary>
    [JsonPropertyName("belowWatermark")]
    public bool BelowWatermark { get; set; }

    /// <summary>Bytes of the storage engine's own files on this volume.</summary>
    [JsonPropertyName("engineBytes")]
    public long EngineBytes { get; set; } = -1;
}

/// <summary>CPU of the server process and of its host.</summary>
public sealed class CamusCpuStats
{
    /// <summary>Process CPU as a share of the whole node (all <see cref="ProcessorCount"/> processors), 0–100.</summary>
    [JsonPropertyName("processCpuPercent")]
    public double ProcessCpuPercent { get; set; } = -1;

    /// <summary>The window <see cref="ProcessCpuPercent"/> is averaged over, in milliseconds.</summary>
    [JsonPropertyName("processCpuWindowMs")]
    public long ProcessCpuWindowMs { get; set; } = -1;

    [JsonPropertyName("processorCount")]
    public int ProcessorCount { get; set; } = -1;

    [JsonPropertyName("loadAverage1m")]
    public double LoadAverage1m { get; set; } = -1;

    [JsonPropertyName("loadAverage5m")]
    public double LoadAverage5m { get; set; } = -1;

    [JsonPropertyName("loadAverage15m")]
    public double LoadAverage15m { get; set; } = -1;
}

/// <summary>Memory of the server process, its managed heap, and its host.</summary>
public sealed class CamusMemoryStats
{
    [JsonPropertyName("processWorkingSetBytes")]
    public long ProcessWorkingSetBytes { get; set; } = -1;

    [JsonPropertyName("gcHeapBytes")]
    public long GcHeapBytes { get; set; } = -1;

    [JsonPropertyName("gcCommittedBytes")]
    public long GcCommittedBytes { get; set; } = -1;

    /// <summary>The managed heap limit in force; <c>0</c> means no limit is in force.</summary>
    [JsonPropertyName("gcMemoryLimitBytes")]
    public long GcMemoryLimitBytes { get; set; } = -1;

    [JsonPropertyName("hostTotalBytes")]
    public long HostTotalBytes { get; set; } = -1;

    [JsonPropertyName("hostAvailableBytes")]
    public long HostAvailableBytes { get; set; } = -1;
}

/// <summary>Why one value is -1. <see cref="Field"/> is the value's path, e.g. <c>cpu.load_average_1m</c>.</summary>
public sealed class CamusNodeStatsError
{
    [JsonPropertyName("field")]
    public string Field { get; set; } = "";

    [JsonPropertyName("message")]
    public string Message { get; set; } = "";
}

/// <summary>The REST envelope of <c>GET /v1/admin/stats</c>.</summary>
internal sealed class CamusNodeStatsResponse
{
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    [JsonPropertyName("code")]
    public string? Code { get; set; }

    [JsonPropertyName("message")]
    public string? Message { get; set; }

    [JsonPropertyName("node")]
    public CamusNodeIdentity? Node { get; set; }

    [JsonPropertyName("disks")]
    public List<CamusDiskVolumeStats>? Disks { get; set; }

    [JsonPropertyName("cpu")]
    public CamusCpuStats? Cpu { get; set; }

    [JsonPropertyName("memory")]
    public CamusMemoryStats? Memory { get; set; }

    [JsonPropertyName("errors")]
    public List<CamusNodeStatsError>? Errors { get; set; }
}
