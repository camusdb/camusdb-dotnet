using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace CamusDB.EntityFrameworkCore;

public class CamusQuerySqlGenerator : QuerySqlGenerator
{
    public CamusQuerySqlGenerator(QuerySqlGeneratorDependencies dependencies)
        : base(dependencies) { }

    protected override void GenerateLimitOffset(SelectExpression selectExpression)
    {
        if (selectExpression.Limit != null)
        {
            Sql.AppendLine().Append("LIMIT ");
            Visit(selectExpression.Limit);
        }

        if (selectExpression.Offset != null)
        {
            if (selectExpression.Limit == null)
                Sql.AppendLine().Append("LIMIT -1");

            Sql.Append(" OFFSET ");
            Visit(selectExpression.Offset);
        }
    }
}
