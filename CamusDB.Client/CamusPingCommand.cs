
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client.Transport;

namespace CamusDB.Client;

public class CamusPingCommand : CamusCommand
{
    public CamusPingCommand(string source, CamusConnectionStringBuilder builder, CamusConnection? connection = null) : base(source, builder, connection)
    {

    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        string endpoint = GetEndpoint();

        bool alive = await builder.GetTransport().PingAsync(endpoint, CommandTimeout, cancellationToken).ConfigureAwait(false);

        return alive ? 1 : 0;
    }
}
