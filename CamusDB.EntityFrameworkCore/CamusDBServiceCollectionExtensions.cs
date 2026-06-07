using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.EntityFrameworkCore;

public static class CamusDBServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkCamusDB(this IServiceCollection serviceCollection)
    {
        ArgumentNullException.ThrowIfNull(serviceCollection);

        EntityFrameworkServicesBuilder builder = new(serviceCollection);

        builder.TryAdd<IDatabaseProvider, DatabaseProvider<CamusDBOptionsExtension>>();
        builder.TryAddCoreServices();

        return serviceCollection;
    }
}
