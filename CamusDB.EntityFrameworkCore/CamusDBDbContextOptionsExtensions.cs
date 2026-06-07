using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.EntityFrameworkCore;

public static class CamusDBDbContextOptionsExtensions
{
    public static DbContextOptionsBuilder UseCamusDB(
        this DbContextOptionsBuilder optionsBuilder,
        string connectionString,
        Action<CamusDBDbContextOptionsBuilder>? camusOptionsAction = null)
    {
        ArgumentNullException.ThrowIfNull(optionsBuilder);
        ArgumentException.ThrowIfNullOrWhiteSpace(connectionString);

        CamusDBOptionsExtension extension = optionsBuilder.Options.FindExtension<CamusDBOptionsExtension>() ?? new CamusDBOptionsExtension();
        extension = extension.WithConnectionString(connectionString);

        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        camusOptionsAction?.Invoke(new CamusDBDbContextOptionsBuilder(optionsBuilder));

        return optionsBuilder;
    }

    public static DbContextOptionsBuilder<TContext> UseCamusDB<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string connectionString,
        Action<CamusDBDbContextOptionsBuilder>? camusOptionsAction = null)
        where TContext : DbContext
    {
        UseCamusDB((DbContextOptionsBuilder)optionsBuilder, connectionString, camusOptionsAction);
        return optionsBuilder;
    }
}
