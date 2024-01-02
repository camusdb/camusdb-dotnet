
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

public class SessionPoolOptions
{
    public int MinimumPooledSessions { get; set; }

    public int MaximumActiveSessions { get; set; }
}
