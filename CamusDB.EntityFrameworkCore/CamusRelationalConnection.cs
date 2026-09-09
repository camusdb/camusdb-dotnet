using System.Data.Common;
using CamusDB.Client;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// EF Core relational connection wrapper over <see cref="CamusConnection"/>.
///
/// Transaction affinity guarantee: when a <see cref="CamusTransaction"/> is active it
/// stores the endpoint it was started on, and every command inside it resolves that pinned
/// endpoint instead of asking the pool for the next one, so the whole transaction is routed to
/// the same node even when multiple endpoints are configured in the connection string. With
/// learned routing on the transaction is started by its first command, on that command's learned
/// endpoint; the provider needs nothing for that, because it never reads the transaction's identity
/// before a command runs.
/// </summary>
public sealed class CamusRelationalConnection : RelationalConnection
{
    public CamusRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies) { }

    protected override DbConnection CreateDbConnection()
    {
        var cs = ConnectionString
            ?? throw new InvalidOperationException("A CamusDB connection string must be configured via UseCamusDB(...).");

        var connection = new CamusConnection(new CamusConnectionStringBuilder(cs));

        // Apply the context's default concurrency options (e.g. UseOptimisticLocking()) so every
        // transaction and autocommit statement EF runs on this connection inherits them.
        var extension = Dependencies.ContextOptions.FindExtension<CamusDBOptionsExtension>();
        if (extension?.DefaultTransactionOptions is { } defaults)
            connection.DefaultTransactionOptions = defaults;

        return connection;
    }

    /// <summary>
    /// Returns the underlying <see cref="CamusConnection"/>, opening it if necessary.
    /// </summary>
    public new CamusConnection DbConnection => (CamusConnection)base.DbConnection;
}
