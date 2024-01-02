
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

public class SessionPoolManager
{
    public static SessionPoolManager Create(SessionPoolOptions options)
    {
        return new SessionPoolManager();
    }
}
