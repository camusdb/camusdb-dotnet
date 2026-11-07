# Backups

`connection.Backups` takes, lists, checks and deletes backups while the server runs. Restore is an operator task; see the server's `backups-and-point-in-time-recovery` guide.

```csharp
// Take a full backup, then an incremental chained onto it.
CamusBackupInfo full = await connection.Backups.TakeFullBackupAsync();
CamusBackupInfo incremental = await connection.Backups.TakeIncrementalBackupAsync(full.BackupId);

// Resolve and validate the chain ending at a leaf. Returned root-first.
IReadOnlyList<CamusBackupInfo> chain = await connection.Backups.GetChainAsync(incremental.BackupId);

// The chain's restorable window is reported on the ROOT entry, not the leaf.
long? from = chain[0].MinRecoverablePhysicalMs;   // Unix epoch milliseconds
long? to   = chain[0].MaxRecoverablePhysicalMs;

// List everything in the catalog.
IReadOnlyList<CamusBackupInfo> catalog = await connection.Backups.ListBackupsAsync();

// Retention: preview first, then apply.
CamusBackupGcResult preview = await connection.Backups.PreviewGarbageCollectionAsync();   // Applied == false
CamusBackupGcResult applied = await connection.Backups.CollectGarbageAsync();
```

An incremental backup holds changes since its parent. A chain is a full backup plus its incrementals.

- A backup covers every database on the node.
- The server needs `kahuna.backup_dir` in `config.yml`. Without it, calls fail with `BackupNotConfigured` (HTTP 503).
- Calls need a superuser. With authentication off, only local callers may call.
- If the parent is too old, an incremental becomes a full backup. Check `WasSubstituted` and `SubstitutionReason`.
- `TakeCoordinatedBackupAsync` (one consistent cluster backup) must reach the coordinator, or fails with `BackupNotCoordinator`.
- `GetChainAsync` fails if the chain cannot restore.
- Backups use REST only. With `Protocol=grpc`, set `BackupEndpoint=` to the HTTP endpoint.

| Key | Default | Meaning |
| --- | --- | --- |
| `BackupEndpoint` | `Endpoint` | Node for backup calls, such as the coordinator. |
| `BackupTimeout` | `300` s | Timeout for backup calls. |
