
/**
 * This file is part of CamusDB
 *
 * Offline coverage for the online backup admin surface: how the backup endpoint and timeout are resolved
 * from the connection string, and that the client's DTOs decode the server's exact camelCase wire shape
 * (CamusDB.App.Models.BackupInfoModel / BackupListResponse / BackupGcResponse). No server is required —
 * nothing here opens a connection.
 */

using System.Text.Json;

namespace CamusDB.Client.Tests;

public class TestBackups
{
    [Fact]
    public void BackupEndpointFallsBackToEndpointOnRest()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db");

        Assert.Equal("http://localhost:5095", builder.GetBackupEndpoint());
    }

    [Fact]
    public void BackupEndpointOverridesEndpoint()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db;BackupEndpoint=http://coordinator:5000");

        Assert.Equal("http://coordinator:5000", builder.GetBackupEndpoint());
    }

    // The backup endpoints are REST-only, and a gRPC connection's Endpoint= addresses the gRPC port.
    // Refusing here names the key to set instead of sending HTTP at a gRPC port and failing obscurely.
    [Fact]
    public void GrpcWithoutBackupEndpointIsRefused()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5096;Database=db;Protocol=grpc");

        CamusException ex = Assert.Throws<CamusException>(() => builder.GetBackupEndpoint());

        Assert.Contains("BackupEndpoint", ex.Message);
    }

    [Fact]
    public void GrpcWithBackupEndpointIsAllowed()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5096;Database=db;Protocol=grpc;BackupEndpoint=http://localhost:5095");

        Assert.Equal("http://localhost:5095", builder.GetBackupEndpoint());
    }

    // A full backup copies a whole node's base image, so it must not inherit the 10s statement timeout.
    [Fact]
    public void BackupTimeoutDefaultsWellAboveCommandTimeout()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db");

        Assert.Equal(300, builder.BackupTimeout);
        Assert.True(builder.BackupTimeout > builder.CommandTimeout);
    }

    [Theory]
    [InlineData("BackupTimeout=60", 60)]
    [InlineData("BackupTimeout=0", 300)]
    [InlineData("BackupTimeout=-5", 300)]
    [InlineData("BackupTimeout=soon", 300)]
    public void BackupTimeoutIsParsedWithFallback(string setting, int expected)
    {
        CamusConnectionStringBuilder builder = new($"Endpoint=http://localhost:5095;Database=db;{setting}");

        Assert.Equal(expected, builder.BackupTimeout);
    }

    [Fact]
    public void BackupInfoDecodesServerWireShape()
    {
        const string json = """
        {
          "status": "ok",
          "backup": {
            "backupId": "0f8fad5b-d9cb-469f-a165-70867728950e",
            "formatVersion": 1,
            "type": "Coordinated",
            "createdAtUtc": "2026-08-09T12:34:56Z",
            "parentBackupId": null,
            "partitionCount": 4,
            "clusterSnapshotNode": 2,
            "clusterSnapshotPhysical": 1750000000000,
            "clusterSnapshotCounter": 7,
            "requestedKind": "Coordinated",
            "actualKind": "Coordinated",
            "substitutionReason": null,
            "isInvalid": false,
            "invalidReason": null,
            "minRecoverablePhysicalMs": null,
            "maxRecoverablePhysicalMs": null,
            "clusterId": "prod-a",
            "coordinatorNode": "node-1"
          }
        }
        """;

        CamusBackupResponse? response = JsonSerializer.Deserialize(json, CamusJsonSerializerContext.Default.CamusBackupResponse);

        Assert.NotNull(response);
        Assert.Equal("ok", response.Status);

        CamusBackupInfo backup = Assert.IsType<CamusBackupInfo>(response.Backup);

        Assert.Equal("0f8fad5b-d9cb-469f-a165-70867728950e", backup.BackupId);
        Assert.Equal(1, backup.FormatVersion);
        Assert.Equal("Coordinated", backup.Type);
        Assert.Equal(new DateTime(2026, 8, 9, 12, 34, 56, DateTimeKind.Utc), backup.CreatedAtUtc.ToUniversalTime());
        Assert.Null(backup.ParentBackupId);
        Assert.Equal(4, backup.PartitionCount);
        Assert.Equal(2, backup.ClusterSnapshotNode);
        Assert.Equal(1750000000000L, backup.ClusterSnapshotPhysical);
        Assert.Equal(7u, backup.ClusterSnapshotCounter);
        Assert.Equal("prod-a", backup.ClusterId);
        Assert.Equal("node-1", backup.CoordinatorNode);
        Assert.False(backup.IsInvalid);
        Assert.False(backup.WasSubstituted);
    }

    // An incremental whose parent aged out is taken as a full instead. The call succeeds, so the only
    // signal that it cost a full image is the requested/actual mismatch.
    [Fact]
    public void SubstitutedIncrementalIsVisible()
    {
        const string json = """
        {
          "backupId": "0f8fad5b-d9cb-469f-a165-70867728950e",
          "type": "Full",
          "requestedKind": "Incremental",
          "actualKind": "Full",
          "substitutionReason": "Parent aged past the retention floor"
        }
        """;

        CamusBackupInfo? backup = JsonSerializer.Deserialize(json, CamusJsonSerializerContext.Default.CamusBackupInfo);

        Assert.NotNull(backup);
        Assert.True(backup.WasSubstituted);
        Assert.Equal("Parent aged past the retention floor", backup.SubstitutionReason);
    }

    // Absent kind fields (an older server, or a listing entry) must not read as a substitution.
    [Fact]
    public void MissingKindFieldsAreNotASubstitution()
    {
        const string json = """{ "backupId": "0f8fad5b-d9cb-469f-a165-70867728950e", "type": "Full" }""";

        CamusBackupInfo? backup = JsonSerializer.Deserialize(json, CamusJsonSerializerContext.Default.CamusBackupInfo);

        Assert.NotNull(backup);
        Assert.False(backup.WasSubstituted);
    }

    // A chain is returned root-first, and the recoverable window is reported on the ROOT rather than the
    // leaf. This literal is the shape a live server actually returns for a two-element chain.
    [Fact]
    public void ChainDecodesRootFirstWithRecoverableWindowOnRoot()
    {
        const string json = """
        {
          "status": "ok",
          "backups": [
            {
              "backupId": "11111111-1111-1111-1111-111111111111",
              "type": "Full",
              "parentBackupId": null,
              "minRecoverablePhysicalMs": 1749999000000,
              "maxRecoverablePhysicalMs": 1750000000000
            },
            {
              "backupId": "22222222-2222-2222-2222-222222222222",
              "type": "Incremental",
              "parentBackupId": "11111111-1111-1111-1111-111111111111",
              "minRecoverablePhysicalMs": null,
              "maxRecoverablePhysicalMs": null
            }
          ]
        }
        """;

        CamusBackupListResponse? response = JsonSerializer.Deserialize(json, CamusJsonSerializerContext.Default.CamusBackupListResponse);

        Assert.NotNull(response);
        Assert.NotNull(response.Backups);
        Assert.Equal(2, response.Backups.Count);

        CamusBackupInfo root = response.Backups[0];
        Assert.Equal("Full", root.Type);
        Assert.Null(root.ParentBackupId);
        Assert.Equal(1749999000000L, root.MinRecoverablePhysicalMs);
        Assert.Equal(1750000000000L, root.MaxRecoverablePhysicalMs);

        CamusBackupInfo leaf = response.Backups[1];
        Assert.Equal("11111111-1111-1111-1111-111111111111", leaf.ParentBackupId);
        Assert.Null(leaf.MinRecoverablePhysicalMs);
    }

    // Listing fails open on one unreadable manifest so a single corrupt entry cannot hide the catalog.
    [Fact]
    public void InvalidCatalogEntryIsSurfacedNotDropped()
    {
        const string json = """
        {
          "status": "ok",
          "backups": [
            { "backupId": "33333333-3333-3333-3333-333333333333", "isInvalid": true, "invalidReason": "manifest unreadable" }
          ]
        }
        """;

        CamusBackupListResponse? response = JsonSerializer.Deserialize(json, CamusJsonSerializerContext.Default.CamusBackupListResponse);

        Assert.NotNull(response);
        CamusBackupInfo entry = Assert.Single(response.Backups!);

        Assert.True(entry.IsInvalid);
        Assert.Equal("manifest unreadable", entry.InvalidReason);
    }

    [Fact]
    public void GcResponseDecodesDeletionsAndOrphans()
    {
        const string json = """
        {
          "status": "ok",
          "applied": false,
          "bytesReclaimed": 4096,
          "retentionDeletions": [
            {
              "backupId": "44444444-4444-4444-4444-444444444444",
              "type": "Full",
              "createdAtUtc": "2026-08-01T00:00:00Z",
              "bytes": 4096,
              "reason": "max_chains"
            }
          ],
          "orphanReclamations": [
            { "name": "tmp-partial", "isDirectory": true, "reason": "not referenced by any manifest" }
          ]
        }
        """;

        CamusBackupGcResponse? response = JsonSerializer.Deserialize(json, CamusJsonSerializerContext.Default.CamusBackupGcResponse);

        Assert.NotNull(response);
        Assert.False(response.Applied);
        Assert.Equal(4096L, response.BytesReclaimed);

        CamusBackupGcDeletion deletion = Assert.Single(response.RetentionDeletions!);
        Assert.Equal("44444444-4444-4444-4444-444444444444", deletion.BackupId);
        Assert.Equal("max_chains", deletion.Reason);
        Assert.Equal(4096L, deletion.Bytes);

        CamusBackupGcOrphan orphan = Assert.Single(response.OrphanReclamations!);
        Assert.Equal("tmp-partial", orphan.Name);
        Assert.True(orphan.IsDirectory);
    }

    // Failures arrive as {status,code,message} with a non-2xx status; the envelope check is the backstop
    // for a body that says failed without one.
    [Fact]
    public void FailureEnvelopeCarriesCodeAndMessage()
    {
        const string json = """{ "status": "failed", "code": "CADB0000", "message": "Backups are not configured" }""";

        CamusBackupResponse? response = JsonSerializer.Deserialize(json, CamusJsonSerializerContext.Default.CamusBackupResponse);

        Assert.NotNull(response);
        Assert.Equal("failed", response.Status);
        Assert.Equal("CADB0000", response.Code);
        Assert.Equal("Backups are not configured", response.Message);
        Assert.Null(response.Backup);
    }

    [Fact]
    public void ConnectionExposesASingleBackupClient()
    {
        using CamusConnection connection = new(new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=db"));

        Assert.Same(connection.Backups, connection.Backups);
    }
}
