using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Non-retrying execution strategy for CamusDB.
/// CamusDB's own endpoint pool handles transport-level failover; EF retries would double-retry
/// and risk re-executing non-idempotent writes. Users who need EF-level retries can subclass
/// this and override <see cref="ShouldRetryOn"/>.
/// </summary>
public class CamusExecutionStrategy : ExecutionStrategy
{
    public CamusExecutionStrategy(ExecutionStrategyDependencies dependencies)
        : base(dependencies, maxRetryCount: 0, maxRetryDelay: TimeSpan.Zero) { }

    protected override bool ShouldRetryOn(Exception exception) => false;
}

public sealed class CamusExecutionStrategyFactory : IExecutionStrategyFactory
{
    private readonly ExecutionStrategyDependencies _dependencies;

    public CamusExecutionStrategyFactory(ExecutionStrategyDependencies dependencies)
        => _dependencies = dependencies;

    public IExecutionStrategy Create() => new CamusExecutionStrategy(_dependencies);
}
