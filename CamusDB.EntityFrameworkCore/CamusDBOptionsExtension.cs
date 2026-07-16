using CamusDB.Client;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusDBOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public CamusDBOptionsExtension() { }

    private CamusDBOptionsExtension(CamusDBOptionsExtension copyFrom) : base(copyFrom)
    {
        RetryOnFailureEnabled = copyFrom.RetryOnFailureEnabled;
        RetryOnFailureCount = copyFrom.RetryOnFailureCount;
        RetryMaxDelay = copyFrom.RetryMaxDelay;
        RetryDeadline = copyFrom.RetryDeadline;
        RetryMedianFirstDelay = copyFrom.RetryMedianFirstDelay;
        DefaultTransactionOptions = copyFrom.DefaultTransactionOptions;
    }

    /// <summary>
    /// Connection-wide default concurrency options (isolation / mode / locking) applied to every
    /// transaction and autocommit statement on contexts using this provider, unless the caller specifies
    /// its own. Set via <see cref="CamusDBDbContextOptionsBuilder.UseOptimisticLocking"/> /
    /// <see cref="CamusDBDbContextOptionsBuilder.UseTransactionDefaults"/>. <see langword="null"/> means
    /// the connection-string / server defaults apply.
    /// </summary>
    public CamusTransactionOptions? DefaultTransactionOptions { get; private set; }

    public bool RetryOnFailureEnabled { get; private set; }

    public int RetryOnFailureCount { get; private set; } = 15;

    public TimeSpan RetryMaxDelay { get; private set; } = TimeSpan.FromSeconds(1);

    public TimeSpan RetryDeadline { get; private set; } = TimeSpan.FromSeconds(5);

    public TimeSpan RetryMedianFirstDelay { get; private set; } = TimeSpan.FromMilliseconds(30);

    protected override RelationalOptionsExtension Clone() => new CamusDBOptionsExtension(this);

    public override DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public override void ApplyServices(IServiceCollection services)
        => CamusDBServiceCollectionExtensions.AddEntityFrameworkCamusDB(services);

    public CamusDBOptionsExtension WithRetryOnFailure(
        int maxRetryCount,
        TimeSpan maxRetryDelay,
        TimeSpan retryDeadline,
        TimeSpan medianFirstRetryDelay)
    {
        if (maxRetryCount < 1)
            throw new ArgumentOutOfRangeException(nameof(maxRetryCount), "Retry count must be at least 1.");

        if (maxRetryDelay <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(maxRetryDelay), "Max retry delay must be greater than zero.");

        if (retryDeadline <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(retryDeadline), "Retry deadline must be greater than zero.");

        if (medianFirstRetryDelay <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(medianFirstRetryDelay), "Median first retry delay must be greater than zero.");

        var clone = (CamusDBOptionsExtension)Clone();
        clone.RetryOnFailureEnabled = true;
        clone.RetryOnFailureCount = maxRetryCount;
        clone.RetryMaxDelay = maxRetryDelay;
        clone.RetryDeadline = retryDeadline;
        clone.RetryMedianFirstDelay = medianFirstRetryDelay;
        return clone;
    }

    public CamusDBOptionsExtension WithTransactionDefaults(CamusTransactionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var clone = (CamusDBOptionsExtension)Clone();
        clone.DefaultTransactionOptions = options;
        return clone;
    }

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension)
        : RelationalOptionsExtension.RelationalExtensionInfo(extension)
    {
        private new CamusDBOptionsExtension Extension => (CamusDBOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => true;

        public override string LogFragment => "using CamusDB ";

        // Service provider configuration is identical for all CamusDB contexts — share it
        public override int GetServiceProviderHashCode() => 0;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            if (Extension.Connection is not null)
                debugInfo["CamusDB:Connection"] = Extension.Connection.GetType().Name;
            else
                debugInfo["CamusDB:ConnectionString"] = Extension.ConnectionString ?? "";

            debugInfo["CamusDB:RetryOnFailureEnabled"] = Extension.RetryOnFailureEnabled.ToString();
            debugInfo["CamusDB:RetryOnFailureCount"] = Extension.RetryOnFailureCount.ToString();
            debugInfo["CamusDB:RetryMaxDelayMs"] = Extension.RetryMaxDelay.TotalMilliseconds.ToString("F0");
            debugInfo["CamusDB:RetryDeadlineMs"] = Extension.RetryDeadline.TotalMilliseconds.ToString("F0");
            debugInfo["CamusDB:RetryMedianFirstDelayMs"] = Extension.RetryMedianFirstDelay.TotalMilliseconds.ToString("F0");

            CamusTransactionOptions? txOptions = Extension.DefaultTransactionOptions;
            debugInfo["CamusDB:DefaultLocking"] = txOptions?.Locking?.ToString() ?? "(default)";
            debugInfo["CamusDB:DefaultIsolationLevel"] = txOptions?.IsolationLevel?.ToString() ?? "(default)";
            debugInfo["CamusDB:DefaultTransactionMode"] = txOptions?.Mode?.ToString() ?? "(default)";
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo;
    }
}
