
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data.Common;

namespace CamusDB.Client;

/// <summary>
/// A <see cref="DbDataSource"/> for one CamusDB connection string: hands out connections and
/// commands without the caller handling the connection string again.
///
/// <para>Every connection gets a builder of its own, parsed from the same connection string, so
/// <see cref="CamusConnection.ChangeDatabase"/> on one connection never moves the others. That costs
/// no extra logins or transports: the driver already shares the token and the transport process-wide
/// between builders with the same deployment, identity and settings.</para>
/// </summary>
public sealed class CamusDataSource : DbDataSource
{
    private readonly string connectionString;

    /// <summary>
    /// Creates a data source for <paramref name="connectionString"/>. The string is parsed here, so a
    /// malformed or insecure one fails now rather than on the first connection.
    /// </summary>
    public CamusDataSource(string connectionString)
    {
        ArgumentNullException.ThrowIfNull(connectionString);

        this.connectionString = new CamusConnectionStringBuilder(connectionString).ToString();
    }

    public override string ConnectionString => connectionString;

    /// <summary>Creates a closed connection for this data source's connection string.</summary>
    public new CamusConnection CreateConnection() => new(new CamusConnectionStringBuilder(connectionString));

    protected override DbConnection CreateDbConnection() => CreateConnection();
}
