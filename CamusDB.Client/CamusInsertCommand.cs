
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client.Transport;

namespace CamusDB.Client;

/// <summary>
/// A typed row insert: <see cref="CamusCommand.CommandText"/> names the target table and the bound
/// parameters name its columns, so no SQL is composed at all.
///
/// <para>It runs through the connection's <see cref="ICamusTransport"/>, exactly as every other
/// statement does. That is what attaches the bearer token, replays a statement whose token the server
/// rejected, marks an endpoint unreachable, and translates a failure into a <see cref="CamusException"/>
/// carrying the server's <c>CADBxxxx</c> code — and it is what makes this command work on a
/// <c>Protocol=grpc</c> connection, which its own hand-built HTTP request never could.</para>
/// </summary>
public class CamusInsertCommand : CamusCommand
{
    public CamusInsertCommand(string source, CamusConnectionStringBuilder builder, CamusConnection? connection = null) : base(source, builder, connection)
    {

    }

    /// <inheritdoc />
    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        ICamusTransport transport = builder.GetTransport();

        TransportInsertRequest request = new()
        {
            Endpoint = GetEndpoint(),
            Database = builder.Config["Database"],
            Table = GetRequestTarget(),
            Values = GetCommandParameters(transport.Protocol),
            TxnIdPT = transaction?.TxnIdPT,
            TxnIdCounter = transaction?.TxnIdCounter,
            StreamSlot = transaction?.StreamSlot,
            TimeoutSeconds = CommandTimeout,
        };

        return await transport.InsertAsync(request, cancellationToken).ConfigureAwait(false);
    }
}
