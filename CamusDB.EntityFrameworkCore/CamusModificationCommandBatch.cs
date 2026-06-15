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
}
