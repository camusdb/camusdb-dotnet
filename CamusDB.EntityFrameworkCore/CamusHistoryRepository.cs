using Microsoft.EntityFrameworkCore.Migrations;

namespace CamusDB.EntityFrameworkCore;

public class CamusHistoryRepository : HistoryRepository
{
    public CamusHistoryRepository(HistoryRepositoryDependencies dependencies)
        : base(dependencies) { }

    // CamusDB will throw if this table doesn't exist — we wrap Exists() in a try-catch
    protected override string ExistsSql
        => $"SELECT {MigrationIdColumnName} FROM {TableName} WHERE 1 = 0";

    // If the query above ran without throwing, the table is there
    protected override bool InterpretExistsResult(object? value) => true;

    public override bool Exists()
    {
        try { return base.Exists(); }
        catch { return false; }
    }

    public override async Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
    {
        try { return await base.ExistsAsync(cancellationToken).ConfigureAwait(false); }
        catch { return false; }
    }

    // CamusDB has no IF NOT EXISTS table syntax; rely on Exists() check + normal CreateScript
    public override string GetCreateIfNotExistsScript() => GetCreateScript();

    // Conditional scripting blocks — CamusDB has no equivalent of DO $$ BEGIN ... END $$
    public override string GetBeginIfExistsScript(string migrationId) => "";

    public override string GetBeginIfNotExistsScript(string migrationId) => "";

    public override string GetEndIfScript() => "";

    // CamusDB has no advisory lock mechanism; migration locking is best-effort
    public override LockReleaseBehavior LockReleaseBehavior => LockReleaseBehavior.Transaction;

    public override IMigrationsDatabaseLock AcquireDatabaseLock()
        => new NoOpMigrationsDatabaseLock(this);

    public override Task<IMigrationsDatabaseLock> AcquireDatabaseLockAsync(
        CancellationToken cancellationToken = default)
        => Task.FromResult<IMigrationsDatabaseLock>(new NoOpMigrationsDatabaseLock(this));

    private sealed class NoOpMigrationsDatabaseLock(IHistoryRepository owner) : IMigrationsDatabaseLock
    {
        public IHistoryRepository HistoryRepository => owner;

        public void ReacquireIfNeeded() { }
        public Task ReacquireIfNeededAsync(CancellationToken cancellationToken = default)
            => Task.CompletedTask;
        public void Dispose() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
