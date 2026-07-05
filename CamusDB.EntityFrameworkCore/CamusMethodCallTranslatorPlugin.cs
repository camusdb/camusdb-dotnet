
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.EntityFrameworkCore.Query;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusMethodCallTranslatorPlugin : IMethodCallTranslatorPlugin
{
    public CamusMethodCallTranslatorPlugin(ISqlExpressionFactory sqlExpressionFactory)
    {
        Translators = [new CamusStringMethodTranslator(sqlExpressionFactory)];
    }

    public IEnumerable<IMethodCallTranslator> Translators { get; }
}
