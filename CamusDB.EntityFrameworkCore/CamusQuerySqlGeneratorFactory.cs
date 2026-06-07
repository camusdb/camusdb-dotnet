using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace CamusDB.EntityFrameworkCore;

public class CamusQuerySqlGeneratorFactory : QuerySqlGeneratorFactory
{
    public CamusQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies) { }

    public override QuerySqlGenerator Create() => new CamusQuerySqlGenerator(Dependencies);
}
