using CamusDB.Client;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Update.Internal;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusModificationCommandBatch(ModificationCommandBatchFactoryDependencies dependencies)
    : SingularModificationCommandBatch(dependencies)
{
    public override void Execute(IRelationalConnection connection)
    {
        try
        {
            base.Execute(connection);
        }
        catch (DbUpdateException ex) when (SerializableRetryHelper.IsRetryable(ex.InnerException))
        {
            throw new DbUpdateConcurrencyException(ex.Message, ex.InnerException);
        }
    }

    public override async Task ExecuteAsync(
        IRelationalConnection connection,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await base.ExecuteAsync(connection, cancellationToken).ConfigureAwait(false);
        }
        catch (DbUpdateException ex) when (SerializableRetryHelper.IsRetryable(ex.InnerException))
        {
            throw new DbUpdateConcurrencyException(ex.Message, ex.InnerException);
        }
    }

    // CamusDB conveys the affected-row count via DbDataReader.RecordsAffected (not a result set), and
    // the update generator emits ResultSetMapping.NoResults, so EF's base pipeline never validates it.
    // For a command that carries a concurrency condition ([ConcurrencyCheck] / [Timestamp]), validate
    // it here: a stale write matches zero rows and must surface as DbUpdateConcurrencyException rather
    // than silently succeeding. Ordinary commands (no concurrency token) keep the lenient behavior.
    protected override void Consume(RelationalDataReader reader)
    {
        base.Consume(reader);
        ThrowIfConcurrencyTokenStale(reader);
    }

    protected override async Task ConsumeAsync(
        RelationalDataReader reader, CancellationToken cancellationToken = default)
    {
        await base.ConsumeAsync(reader, cancellationToken).ConfigureAwait(false);
        ThrowIfConcurrencyTokenStale(reader);
    }

    private void ThrowIfConcurrencyTokenStale(RelationalDataReader reader)
    {
        if (ModificationCommands.Count != 1)
            return;

        var command = ModificationCommands[0];

        // A concurrency condition is a non-key column added to the WHERE (a concurrency token).
        bool hasConcurrencyCondition = command.ColumnModifications.Any(m => m.IsCondition && !m.IsKey);
        if (!hasConcurrencyCondition)
            return;

        if (reader.DbDataReader.RecordsAffected == 0)
            throw new DbUpdateConcurrencyException(
                "The database operation was expected to affect 1 row(s), but actually affected 0 row(s); " +
                "data may have been modified or deleted since entities were loaded.",
                command.Entries);
    }
}
