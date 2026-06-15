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
