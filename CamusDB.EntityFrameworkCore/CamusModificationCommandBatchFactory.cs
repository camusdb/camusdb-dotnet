using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Update.Internal;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Creates single-row modification command batches for CamusDB.
/// Uses EF Core's <see cref="SingularModificationCommandBatch"/> so that
/// the provider DI graph boots correctly before the real batch implementation
/// is wired up in Milestone 3.
/// </summary>
public sealed class CamusModificationCommandBatchFactory : IModificationCommandBatchFactory
{
    private readonly ModificationCommandBatchFactoryDependencies _dependencies;

    public CamusModificationCommandBatchFactory(ModificationCommandBatchFactoryDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public ModificationCommandBatch Create() => new SingularModificationCommandBatch(_dependencies);
}
