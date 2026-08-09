
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// Ignored. Both knobs describe a session pool CamusDB does not have — see <see cref="SessionPoolManager"/>
/// for why, and for the <c>ChannelPoolSize=</c> key that tunes the thing Spanner's <c>NumChannels</c> tunes.
/// </summary>
[Obsolete("CamusDB has no server-side sessions to pool; these values are ignored. Tune the gRPC stream pool with the ChannelPoolSize= connection-string key instead.")]
public class SessionPoolOptions
{
    public int MinimumPooledSessions { get; set; }

    public int MaximumActiveSessions { get; set; }
}
