using Microsoft.EntityFrameworkCore.Query;

namespace CamusDB.EntityFrameworkCore;

public class CamusQuerySqlGeneratorFactory : IQuerySqlGeneratorFactory
{
    private readonly QuerySqlGeneratorDependencies _dependencies;

    public CamusQuerySqlGeneratorFactory(QuerySqlGeneratorDependencies dependencies)
    {
        _dependencies = dependencies;
    }

    public QuerySqlGenerator Create() => new CamusQuerySqlGenerator(_dependencies);
}
