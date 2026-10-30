
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusMethodCallTranslatorPlugin : IMethodCallTranslatorPlugin
{
    public CamusMethodCallTranslatorPlugin(ISqlExpressionFactory sqlExpressionFactory, IRelationalTypeMappingSource typeMappingSource)
    {
        Translators =
        [
            new CamusStringMethodTranslator(sqlExpressionFactory),
            new CamusRegexMethodTranslator(sqlExpressionFactory),
            new CamusVectorMethodTranslator(sqlExpressionFactory, typeMappingSource),
        ];
    }

    public IEnumerable<IMethodCallTranslator> Translators { get; }
}
