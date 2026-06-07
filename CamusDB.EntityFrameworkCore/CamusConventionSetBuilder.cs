using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace CamusDB.EntityFrameworkCore;

public class CamusConventionSetBuilder : RelationalConventionSetBuilder
{
    public CamusConventionSetBuilder(
        ProviderConventionSetBuilderDependencies dependencies,
        RelationalConventionSetBuilderDependencies relationalDependencies)
        : base(dependencies, relationalDependencies) { }
}
