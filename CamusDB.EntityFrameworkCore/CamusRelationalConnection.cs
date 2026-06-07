using System.Data.Common;
using CamusDB.Client;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// EF Core relational connection wrapper over <see cref="CamusConnection"/>.
///
/// Transaction affinity guarantee: when a <see cref="CamusTransaction"/> is active it
/// stores the endpoint it was started on. <see cref="CamusCommand.GetEndpoint"/> uses
/// that pinned endpoint instead of asking the pool for the next one, so every command
/// inside a transaction is routed to the same node even when multiple endpoints are
/// configured in the connection string.
/// </summary>
public sealed class CamusRelationalConnection : RelationalConnection
{
    public CamusRelationalConnection(RelationalConnectionDependencies dependencies)
        : base(dependencies) { }

    protected override DbConnection CreateDbConnection()
    {
        var cs = ConnectionString
            ?? throw new InvalidOperationException("A CamusDB connection string must be configured via UseCamusDB(...).");

        return new CamusConnection(new CamusConnectionStringBuilder(cs));
    }

    /// <summary>
    /// Returns the underlying <see cref="CamusConnection"/>, opening it if necessary.
    /// </summary>
    public new CamusConnection DbConnection => (CamusConnection)base.DbConnection;
}
