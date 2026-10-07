/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.EntityFrameworkCore.Query;

namespace CamusDB.EntityFrameworkCore;

public class CamusQueryableMethodTranslatingExpressionVisitorFactory : IQueryableMethodTranslatingExpressionVisitorFactory
{
    public CamusQueryableMethodTranslatingExpressionVisitorFactory(
        QueryableMethodTranslatingExpressionVisitorDependencies dependencies,
        RelationalQueryableMethodTranslatingExpressionVisitorDependencies relationalDependencies)
    {
        Dependencies = dependencies;
        RelationalDependencies = relationalDependencies;
    }

    protected virtual QueryableMethodTranslatingExpressionVisitorDependencies Dependencies { get; }

    protected virtual RelationalQueryableMethodTranslatingExpressionVisitorDependencies RelationalDependencies { get; }

    public virtual QueryableMethodTranslatingExpressionVisitor Create(QueryCompilationContext queryCompilationContext)
        => new CamusQueryableMethodTranslatingExpressionVisitor(
            Dependencies, RelationalDependencies, (RelationalQueryCompilationContext)queryCompilationContext);
}
