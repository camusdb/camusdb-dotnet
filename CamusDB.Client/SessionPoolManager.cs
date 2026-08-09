
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// A no-op kept only so code written against a Spanner-shaped API still compiles.
///
/// <para>CamusDB has no server-side session object to pool: there is nothing to create ahead of a
/// statement, nothing that expires while idle, and no per-node session ceiling. Authentication is a
/// bearer token minted once per deployment and refreshed as needed, and a transaction is started per
/// command — so <see cref="SessionPoolOptions"/> has nothing to size and this type does nothing with it.
/// </para>
///
/// <para>What Spanner's <c>NumChannels</c> tunes does have an equivalent here — the number of long-lived
/// <c>BatchExecute</c> streams per endpoint — and it is set with the <c>ChannelPoolSize=</c>
/// connection-string key instead.</para>
/// </summary>
[Obsolete("CamusDB has no server-side sessions to pool; this type does nothing. Tune the gRPC stream pool with the ChannelPoolSize= connection-string key instead.")]
public class SessionPoolManager
{
    public static SessionPoolManager Create(SessionPoolOptions options)
    {
        return new SessionPoolManager();
    }
}
