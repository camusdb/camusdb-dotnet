# Node stats

`connection.GetNodeStatsAsync()` returns the disk, CPU and memory of one node, over REST (`GET /v1/admin/stats`) or gRPC (`GetNodeStats`).

```csharp
CamusNodeStats stats = await connection.GetNodeStatsAsync();     // the next endpoint in rotation
CamusDiskVolumeStats data = stats.DataVolume!;                    // the volume that holds data_dir

Console.WriteLine($"{data.AvailableBytes} bytes free of {data.TotalBytes}");
Console.WriteLine($"writes refused (below min_free_disk_bytes): {data.BelowWatermark}");

foreach (string endpoint in connection.Endpoints)                 // one call per node of a cluster
{
    CamusNodeStats node = await connection.GetNodeStatsAsync(endpoint);
    Console.WriteLine($"{node.Address}: {node.Cpu.ProcessCpuPercent:F1}% CPU, {node.Memory.HostAvailableBytes} bytes of memory free");
}
```

- `Address` is the endpoint that answered. `GetNodeStatsAsync(endpoint)` accepts only endpoints in `Endpoints`.
- `-1` means unknown; `Errors` names the field and reason. A failed probe never fails the call.
- While `BelowWatermark` is `true`, writes on that node fail with `CADB0536`.
- With authentication on, the account needs superuser or `MONITOR` (`GRANT MONITOR ON *.* TO monitoring;`), or gets `CADB0517`.
- Calls are cheap enough to poll every minute. See the server's `node-stats` guide for all fields.
