
/**
 * This file is part of CamusDB
 *
 * End-to-end coverage for the online backup admin API against a real server started with
 * kahuna.backup_dir set. Opt-in: the whole class no-ops unless CAMUSDB_TEST_BACKUPS=true, because a
 * server without backup_dir answers every one of these endpoints with 503 BackupNotConfigured, and the
 * shared CI instance runs without it.
 *
 * To run it, start a server whose config.yml sets kahuna.backup_dir (the directory must be owner-only,
 * 0700, or the server refuses with BackupInsecureRoot), then:
 *   export CAMUSDB_TEST_BACKUPS=true
 *   dotnet test --filter FullyQualifiedName~TestBackupsLive
 */

namespace CamusDB.Client.Tests;

public sealed class TestBackupsLive
{
    private static bool Configured
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_BACKUPS"), "true", StringComparison.OrdinalIgnoreCase);

    private static CamusConnection Connect()
        => new(new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=test"));

    [Fact]
    public async Task TakesAFullBackupAndFindsItInTheCatalog()
    {
        if (!Configured)
            return;

        using CamusConnection connection = Connect();

        CamusBackupInfo backup = await connection.Backups.TakeFullBackupAsync();

        Assert.NotEqual("", backup.BackupId);
        Assert.Equal("Full", backup.Type);
        Assert.Null(backup.ParentBackupId);
        Assert.False(backup.IsInvalid);
        Assert.False(backup.WasSubstituted);
        Assert.True(backup.CreatedAtUtc > DateTime.UtcNow.AddHours(-1));

        IReadOnlyList<CamusBackupInfo> catalog = await connection.Backups.ListBackupsAsync();

        Assert.Contains(catalog, entry => entry.BackupId == backup.BackupId);
    }

    [Fact]
    public async Task TakesAnIncrementalChainedOnAFull()
    {
        if (!Configured)
            return;

        using CamusConnection connection = Connect();

        CamusBackupInfo full = await connection.Backups.TakeFullBackupAsync();
        CamusBackupInfo incremental = await connection.Backups.TakeIncrementalBackupAsync(full.BackupId);

        // The server substitutes a full when the parent can no longer be based on; assert the parent link
        // only when it actually produced an increment, so an aged-out parent is not a spurious failure.
        if (incremental.WasSubstituted)
            Assert.NotNull(incremental.SubstitutionReason);
        else
            Assert.Equal(full.BackupId, incremental.ParentBackupId);
    }

    [Fact]
    public async Task ResolvesAChainRootFirstWithARecoverableWindow()
    {
        if (!Configured)
            return;

        using CamusConnection connection = Connect();

        CamusBackupInfo full = await connection.Backups.TakeFullBackupAsync();
        CamusBackupInfo leaf = await connection.Backups.TakeIncrementalBackupAsync(full.BackupId);

        IReadOnlyList<CamusBackupInfo> chain = await connection.Backups.GetChainAsync(leaf.BackupId);

        Assert.NotEmpty(chain);
        Assert.Null(chain[0].ParentBackupId);                  // root-first: the base image leads
        Assert.Equal(leaf.BackupId, chain[^1].BackupId);

        // The chain's recoverable coverage is reported on the ROOT, not the leaf — verified against a
        // live server, where a two-element chain carries min/max on [0] and null on the incremental.
        CamusBackupInfo root = chain[0];
        Assert.NotNull(root.MinRecoverablePhysicalMs);
        Assert.NotNull(root.MaxRecoverablePhysicalMs);
        Assert.True(root.MinRecoverablePhysicalMs <= root.MaxRecoverablePhysicalMs);
    }

    [Fact]
    public async Task TakesACoordinatedBackup()
    {
        if (!Configured)
            return;

        using CamusConnection connection = Connect();

        CamusBackupInfo backup = await connection.Backups.TakeCoordinatedBackupAsync();

        Assert.NotEqual("", backup.BackupId);
        Assert.False(backup.IsInvalid);
    }

    [Fact]
    public async Task PreviewingRetentionDeletesNothing()
    {
        if (!Configured)
            return;

        using CamusConnection connection = Connect();

        await connection.Backups.TakeFullBackupAsync();

        int before = (await connection.Backups.ListBackupsAsync()).Count;

        CamusBackupGcResult preview = await connection.Backups.PreviewGarbageCollectionAsync();

        Assert.False(preview.Applied);
        Assert.Equal(before, (await connection.Backups.ListBackupsAsync()).Count);
    }

    [Fact]
    public async Task RunningRetentionIsApplied()
    {
        if (!Configured)
            return;

        using CamusConnection connection = Connect();

        CamusBackupGcResult result = await connection.Backups.CollectGarbageAsync();

        Assert.True(result.Applied);
        Assert.True(result.BytesReclaimed >= 0);
    }

    // A malformed leaf must surface the server's CADBxxxx code, not a raw HTTP failure.
    [Fact]
    public async Task InvalidChainIdSurfacesACamusException()
    {
        if (!Configured)
            return;

        using CamusConnection connection = Connect();

        CamusException ex = await Assert.ThrowsAsync<CamusException>(
            () => connection.Backups.GetChainAsync("not-a-guid"));

        Assert.StartsWith("CADB", ex.Code);
    }

    [Fact]
    public async Task UnknownChainLeafSurfacesACamusException()
    {
        if (!Configured)
            return;

        using CamusConnection connection = Connect();

        CamusException ex = await Assert.ThrowsAsync<CamusException>(
            () => connection.Backups.GetChainAsync(Guid.NewGuid()));

        Assert.StartsWith("CADB", ex.Code);
    }
}
