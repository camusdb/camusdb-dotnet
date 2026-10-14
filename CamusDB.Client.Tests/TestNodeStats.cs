/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using CamusDB.Client.Transport;
using CamusDB.Grpc;

namespace CamusDB.Client.Tests;

/// <summary>
/// <see cref="CamusConnection.GetNodeStatsAsync(CancellationToken)"/>: the wire-to-model mapping of both
/// transports, the refusal to send credentials to an unconfigured endpoint, and — against the live test
/// server — a real answer over REST and over gRPC.
/// </summary>
public class TestNodeStats : BaseTest
{
    [Fact]
    public void Grpc_ReplyMapsOntoTheModel_AndUnsetSectionsStayUnknown()
    {
        NodeStatsReply reply = new()
        {
            Node = new NodeIdentity { NodeId = "1", Endpoint = "db1:7070", Version = "0.15.1", ClusterMode = true, UptimeSeconds = 42, SampledAtUnixMs = 1700000000000 },
            Cpu = new CpuStats { ProcessCpuPercent = 12.5, ProcessCpuWindowMs = 60000, ProcessorCount = 8, LoadAverage1M = 1, LoadAverage5M = 2, LoadAverage15M = 3 },
        };
        reply.Disks.Add(new DiskVolumeStats
        {
            Role = "data", Path = "/var/lib/camusdb", MountPoint = "/var/lib/camusdb", TotalBytes = 100, FreeBytes = 40,
            AvailableBytes = 30, UsedBytes = 60, MinFreeDiskBytes = 64, BelowWatermark = true, EngineBytes = 7,
        });
        reply.Errors.Add(new NodeStatsError { Field = "memory.gc_committed_bytes", Message = "no garbage collection has completed yet" });

        CamusNodeStats stats = GrpcTransport.ToNodeStats("http://db1:5096", reply);

        Assert.Equal("http://db1:5096", stats.Address);
        Assert.Equal("db1:7070", stats.Node.Endpoint);
        Assert.True(stats.Node.ClusterMode);
        Assert.Equal(42, stats.Node.UptimeSeconds);

        CamusDiskVolumeStats data = Assert.Single(stats.Disks);
        Assert.Same(data, stats.DataVolume);
        Assert.Equal(30, data.AvailableBytes);
        Assert.Equal(64, data.MinFreeDiskBytes);
        Assert.True(data.BelowWatermark);
        Assert.Equal(7, data.EngineBytes);

        Assert.Equal(12.5, stats.Cpu.ProcessCpuPercent);
        Assert.Equal(3, stats.Cpu.LoadAverage15m);

        // The server sent no memory section: the model keeps its -1 "unknown" defaults, not zeros.
        Assert.Equal(-1, stats.Memory.HostTotalBytes);
        Assert.Equal(-1, stats.Memory.GcMemoryLimitBytes);

        CamusNodeStatsError error = Assert.Single(stats.Errors);
        Assert.Equal("memory.gc_committed_bytes", error.Field);
    }

    [Fact]
    public void Rest_TheServersJsonDeserializesThroughTheSourceGeneratedContext()
    {
        // The shape GET /v1/admin/stats returns.
        const string body = """
            {"status":"ok",
             "node":{"nodeId":"1","endpoint":"localhost:0","version":"0.15.1","clusterMode":false,"uptimeSeconds":8,"sampledAtUnixMs":1791981918280},
             "disks":[{"role":"data","path":"/data","mountPoint":"/","totalBytes":245107195904,"freeBytes":3068727296,
                       "availableBytes":3068727296,"usedBytes":242038468608,"minFreeDiskBytes":67108864,"belowWatermark":false,"engineBytes":272584}],
             "cpu":{"processCpuPercent":2.43,"processCpuWindowMs":5001,"processorCount":8,"loadAverage1m":4.96,"loadAverage5m":4.24,"loadAverage15m":3.53},
             "memory":{"processWorkingSetBytes":158187520,"gcHeapBytes":24887640,"gcCommittedBytes":-1,"gcMemoryLimitBytes":0,
                       "hostTotalBytes":8589934592,"hostAvailableBytes":2662879695},
             "errors":[{"field":"memory.gc_committed_bytes","message":"no garbage collection has completed yet"}]}
            """;

        CamusNodeStatsResponse? response = JsonSerializer.Deserialize(body, CamusJsonSerializerContext.Default.CamusNodeStatsResponse);

        Assert.NotNull(response);
        Assert.Equal("ok", response!.Status);
        Assert.Equal(67108864, response.Disks![0].MinFreeDiskBytes);
        Assert.Equal(3068727296, response.Disks[0].AvailableBytes);
        Assert.Equal(4.96, response.Cpu!.LoadAverage1m);
        Assert.Equal(0, response.Memory!.GcMemoryLimitBytes);
        Assert.Equal(-1, response.Memory.GcCommittedBytes);
        Assert.Equal("memory.gc_committed_bytes", Assert.Single(response.Errors!).Field);
    }

    [Fact]
    public async Task AnEndpointThatIsNotConfigured_IsRefusedBeforeAnyCall()
    {
        await using CamusConnection connection = new(new CamusConnectionStringBuilder(
            "Endpoint=http://db1:5095,http://db2:5095;Database=db"));

        Assert.Equal(["http://db1:5095", "http://db2:5095"], connection.Endpoints);

        await Assert.ThrowsAsync<ArgumentException>(() => connection.GetNodeStatsAsync("http://attacker:5095"));
    }

    [Theory]
    [InlineData("Endpoint=http://localhost:5095;Database=test")]
    [InlineData("Endpoint=http://localhost:5096;Database=test;Protocol=grpc")]
    public async Task Live_ReturnsTheNodesDiskCpuAndMemory(string connectionString)
    {
        await using CamusConnection connection = new(new CamusConnectionStringBuilder(connectionString));

        CamusNodeStats stats = await connection.GetNodeStatsAsync();

        Assert.Equal(connection.Endpoints[0], stats.Address);
        Assert.NotEmpty(stats.Node.Version);

        CamusDiskVolumeStats data = Assert.IsType<CamusDiskVolumeStats>(stats.DataVolume);
        Assert.True(data.TotalBytes > 0);
        Assert.True(data.AvailableBytes >= 0);
        Assert.Equal(data.TotalBytes - data.FreeBytes, data.UsedBytes);
        Assert.True(data.MinFreeDiskBytes >= 0);

        Assert.True(stats.Cpu.ProcessorCount > 0);
        Assert.True(stats.Memory.ProcessWorkingSetBytes > 0);
    }
}
