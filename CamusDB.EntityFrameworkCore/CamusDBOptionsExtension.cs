using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusDBOptionsExtension : IDbContextOptionsExtension
{
    private DbContextOptionsExtensionInfo? info;

    public string? ConnectionString { get; private set; }

    public DbContextOptionsExtensionInfo Info => info ??= new ExtensionInfo(this);

    public void ApplyServices(IServiceCollection services)
    {
        CamusDBServiceCollectionExtensions.AddEntityFrameworkCamusDB(services);
    }

    public void Validate(IDbContextOptions options)
    {
        if (string.IsNullOrWhiteSpace(ConnectionString))
            throw new InvalidOperationException("A CamusDB connection string is required.");
    }

    public CamusDBOptionsExtension WithConnectionString(string connectionString)
    {
        CamusDBOptionsExtension clone = new()
        {
            ConnectionString = connectionString
        };

        return clone;
    }

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
    {
        private new CamusDBOptionsExtension Extension => (CamusDBOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => true;

        public override string LogFragment =>
            string.IsNullOrWhiteSpace(Extension.ConnectionString)
                ? "using CamusDB "
                : "using CamusDB ";

        public override int GetServiceProviderHashCode() =>
            Extension.ConnectionString?.GetHashCode(StringComparison.Ordinal) ?? 0;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["CamusDB:ConnectionString"] = Extension.ConnectionString ?? "";
        }

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) =>
            other is ExtensionInfo otherInfo &&
            string.Equals(Extension.ConnectionString, otherInfo.Extension.ConnectionString, StringComparison.Ordinal);
    }
}
