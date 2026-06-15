using CamusDB.Client;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Retry strategy for CamusDB transaction conflicts.
/// Transport-level endpoint failover remains handled by CamusDB's own endpoint pool; EF retries
/// are opt-in and only target transient transaction conflicts/aborts.
/// </summary>
public class CamusExecutionStrategy : ExecutionStrategy
{
    private readonly bool _retryOnFailureEnabled;
    private readonly int _maxRetryCount;
    private readonly TimeSpan _maxRetryDelay;
    private readonly TimeSpan _retryDeadline;
    private readonly TimeSpan _medianFirstRetryDelay;
    private DateTime _firstFailureUtc;
    private bool _hasFailure;
    private double _previousDelayMs;

    public CamusExecutionStrategy(ExecutionStrategyDependencies dependencies)
        : this(dependencies, null) { }

    public CamusExecutionStrategy(
        ExecutionStrategyDependencies dependencies,
        CamusDBOptionsExtension? extension)
        : base(
            dependencies,
            maxRetryCount: extension?.RetryOnFailureEnabled == true ? extension.RetryOnFailureCount : 0,
            maxRetryDelay: extension?.RetryOnFailureEnabled == true ? extension.RetryMaxDelay : TimeSpan.Zero)
    {
        _retryOnFailureEnabled = extension?.RetryOnFailureEnabled == true;
        _maxRetryCount = extension?.RetryOnFailureCount ?? 0;
        _maxRetryDelay = extension?.RetryMaxDelay ?? TimeSpan.Zero;
        _retryDeadline = extension?.RetryDeadline ?? TimeSpan.Zero;
        _medianFirstRetryDelay = extension?.RetryMedianFirstDelay ?? TimeSpan.Zero;
        _previousDelayMs = Math.Max(1d, _medianFirstRetryDelay.TotalMilliseconds);
    }

    protected override bool ShouldRetryOn(Exception exception)
        => _retryOnFailureEnabled && IsRetryable(exception);

    protected override void OnFirstExecution()
    {
        base.OnFirstExecution();
        _hasFailure = false;
        _previousDelayMs = Math.Max(1d, _medianFirstRetryDelay.TotalMilliseconds);
    }

    protected override TimeSpan? GetNextDelay(Exception lastException)
    {
        if (!_retryOnFailureEnabled || !IsRetryable(lastException))
            return null;

        if (ExceptionsEncountered.Count > _maxRetryCount)
            return null;

        var now = DateTime.UtcNow;

        if (!_hasFailure)
        {
            _hasFailure = true;
            _firstFailureUtc = now;
        }

        var elapsed = now - _firstFailureUtc;

        if (elapsed >= _retryDeadline)
            return null;

        var nextDelay = ComputeNextDelay();

        if (elapsed + nextDelay > _retryDeadline)
            return null;

        return nextDelay;
    }

    private TimeSpan ComputeNextDelay()
    {
        var minDelayMs = Math.Max(1d, _medianFirstRetryDelay.TotalMilliseconds);
        var maxDelayMs = Math.Max(minDelayMs, _maxRetryDelay.TotalMilliseconds);
        var upperBoundMs = Math.Min(maxDelayMs, Math.Max(minDelayMs, _previousDelayMs * 3d));
        var nextDelayMs = minDelayMs + (Random.Shared.NextDouble() * (upperBoundMs - minDelayMs));

        _previousDelayMs = nextDelayMs;

        return TimeSpan.FromMilliseconds(nextDelayMs);
    }

    private static bool IsRetryable(Exception? exception)
        => SerializableRetryHelper.IsRetryable(exception);
}

public sealed class CamusExecutionStrategyFactory : IExecutionStrategyFactory
{
    private readonly ExecutionStrategyDependencies _dependencies;
    private readonly CamusDBOptionsExtension? _extension;

    public CamusExecutionStrategyFactory(
        ExecutionStrategyDependencies dependencies,
        IDbContextOptions options)
    {
        _dependencies = dependencies;
        _extension = options.FindExtension<CamusDBOptionsExtension>();
    }

    public IExecutionStrategy Create() => new CamusExecutionStrategy(_dependencies, _extension);
}
