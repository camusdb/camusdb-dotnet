
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data.Common;

namespace CamusDB.Client;

/// <summary>
/// The <see cref="DbProviderFactory"/> for CamusDB: creates the provider's connections, commands,
/// parameters and data sources for code that works against ADO.NET's abstract types, such as
/// <see cref="DbProviderFactories"/>, Testcontainers' database fixtures or health-check libraries.
///
/// <para>Register it under the invariant name <see cref="InvariantName"/> to resolve it by name:
/// <c>DbProviderFactories.RegisterFactory(CamusProviderFactory.InvariantName, CamusProviderFactory.Instance)</c>.</para>
///
/// <para><see cref="CreateConnectionStringBuilder"/> returns <see langword="null"/>, the base behavior:
/// <see cref="CamusConnectionStringBuilder"/> is not a <see cref="DbConnectionStringBuilder"/>.</para>
/// </summary>
public sealed class CamusProviderFactory : DbProviderFactory
{
    /// <summary>The provider's invariant name, for <see cref="DbProviderFactories"/>.</summary>
    public const string InvariantName = "CamusDB.Client";

    /// <summary>
    /// The single instance. A public static field named <c>Instance</c> is what
    /// <see cref="DbProviderFactories.RegisterFactory(string, Type)"/> looks for.
    /// </summary>
    public static readonly CamusProviderFactory Instance = new();

    private CamusProviderFactory()
    {
    }

    /// <summary>
    /// Creates a connection without a connection string. Assign
    /// <see cref="CamusConnection.ConnectionString"/> once before using it.
    /// </summary>
    public override DbConnection CreateConnection() => new CamusConnection();

    /// <summary>
    /// Creates a command without a connection. It runs against the connection assigned to
    /// <see cref="DbCommand.Connection"/>.
    /// </summary>
    public override DbCommand CreateCommand() => new CamusCommand("", new CamusConnectionStringBuilder(""));

    public override DbParameter CreateParameter() => new CamusParameter();

    /// <summary>
    /// Creates a <see cref="CamusDataSource"/>. The base implementation would assign the connection
    /// string to a connection after creating it, which a <see cref="CamusConnection"/> built from a
    /// connection string does not allow.
    /// </summary>
    public override DbDataSource CreateDataSource(string connectionString) => new CamusDataSource(connectionString);
}
