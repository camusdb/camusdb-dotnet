using CamusDB.Client;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusDBDbContextOptionsBuilder
{
    public CamusDBDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    {
        OptionsBuilder = optionsBuilder;
    }

    public DbContextOptionsBuilder OptionsBuilder { get; }

    /// <summary>
    /// Makes transactions and autocommit statements on this context use <b>optimistic</b> locking by
    /// default: no explicit locks are taken and write–write / read–write conflicts are detected at commit.
    /// A losing transaction surfaces as a conflict — pair this with <c>EnableRetryOnFailure()</c> so EF
    /// replays it. Overridable per transaction; leaves isolation level and mode untouched.
    /// </summary>
    public CamusDBDbContextOptionsBuilder UseOptimisticLocking()
        => UseTransactionDefaults(new CamusTransactionOptions { Locking = CamusLocking.Optimistic });

    /// <summary>Makes transactions on this context use <b>pessimistic</b> locking by default (the engine default).</summary>
    public CamusDBDbContextOptionsBuilder UsePessimisticLocking()
        => UseTransactionDefaults(new CamusTransactionOptions { Locking = CamusLocking.Pessimistic });

    /// <summary>
    /// Sets the connection-wide default concurrency options (isolation level, read/write mode, locking)
    /// for this context. Any knob left <see langword="null"/> defers to the connection-string / server
    /// default; a per-transaction selection overrides these.
    /// </summary>
    public CamusDBDbContextOptionsBuilder UseTransactionDefaults(CamusTransactionOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var infrastructure = (IDbContextOptionsBuilderInfrastructure)OptionsBuilder;
        var extension = (CamusDBOptionsExtension)(
            OptionsBuilder.Options.FindExtension<CamusDBOptionsExtension>() ?? new CamusDBOptionsExtension()
        ).WithTransactionDefaults(options);

        infrastructure.AddOrUpdateExtension(extension);
        return this;
    }

    public CamusDBDbContextOptionsBuilder EnableRetryOnFailure(
        int maxRetryCount = 15,
        TimeSpan? maxRetryDelay = null,
        TimeSpan? retryDeadline = null,
        TimeSpan? medianFirstRetryDelay = null)
    {
        var infrastructure = (IDbContextOptionsBuilderInfrastructure)OptionsBuilder;
        var extension = (CamusDBOptionsExtension)(
            OptionsBuilder.Options.FindExtension<CamusDBOptionsExtension>() ?? new CamusDBOptionsExtension()
        ).WithRetryOnFailure(
            maxRetryCount,
            maxRetryDelay ?? TimeSpan.FromSeconds(1),
            retryDeadline ?? TimeSpan.FromSeconds(5),
            medianFirstRetryDelay ?? TimeSpan.FromMilliseconds(30));

        infrastructure.AddOrUpdateExtension(extension);
        return this;
    }
}
