using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusDBOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public CamusDBOptionsExtension() { }

    private CamusDBOptionsExtension(CamusDBOptionsExtension copyFrom) : base(copyFrom) { }

    protected override RelationalOptionsExtension Clone() => new CamusDBOptionsExtension(this);

    public override DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public override void ApplyServices(IServiceCollection services)
        => CamusDBServiceCollectionExtensions.AddEntityFrameworkCamusDB(services);

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension)
        : RelationalOptionsExtension.RelationalExtensionInfo(extension)
    {
        private new CamusDBOptionsExtension Extension => (CamusDBOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => true;

        public override string LogFragment => "using CamusDB ";

        public override int GetServiceProviderHashCode()
            => Extension.ConnectionString?.GetHashCode(StringComparison.Ordinal) ?? 0;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
            => debugInfo["CamusDB:ConnectionString"] = Extension.ConnectionString ?? "";

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
            => other is ExtensionInfo otherInfo &&
               string.Equals(Extension.ConnectionString, otherInfo.Extension.ConnectionString, StringComparison.Ordinal);
    }
}
