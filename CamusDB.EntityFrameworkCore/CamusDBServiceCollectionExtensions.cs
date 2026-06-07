using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.EntityFrameworkCore;

public static class CamusDBServiceCollectionExtensions
{
    public static IServiceCollection AddEntityFrameworkCamusDB(this IServiceCollection serviceCollection)
    {
        ArgumentNullException.ThrowIfNull(serviceCollection);

        var builder = new EntityFrameworkRelationalServicesBuilder(serviceCollection);

        builder.TryAdd<IDatabaseProvider, DatabaseProvider<CamusDBOptionsExtension>>();
        builder.TryAdd<LoggingDefinitions, CamusLoggingDefinitions>();
        builder.TryAdd<ISqlGenerationHelper, CamusSqlGenerationHelper>();
        builder.TryAdd<IRelationalTypeMappingSource, CamusTypeMappingSource>();
        builder.TryAdd<IRelationalConnection, CamusRelationalConnection>();
        builder.TryAdd<IModelValidator, CamusModelValidator>();
        builder.TryAdd<IUpdateSqlGenerator, CamusUpdateSqlGenerator>();
        builder.TryAdd<IModificationCommandBatchFactory, CamusModificationCommandBatchFactory>();
        builder.TryAdd<IValueGeneratorSelector, CamusValueGeneratorSelector>();
        builder.TryAdd<IRelationalDatabaseCreator, CamusDatabaseCreator>();
        builder.TryAdd<IMigrationsSqlGenerator, CamusMigrationsSqlGenerator>();
        builder.TryAdd<IHistoryRepository, CamusHistoryRepository>();
        builder.TryAdd<IExecutionStrategyFactory, CamusExecutionStrategyFactory>();
        builder.TryAddCoreServices();

        return serviceCollection;
    }
}
