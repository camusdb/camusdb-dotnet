
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Transport;

/// <summary>
/// The transport seam between the ADO.NET surface (<see cref="CamusConnection"/>, <see cref="CamusCommand"/>,
/// <see cref="CamusTransaction"/>) and the wire protocol. One implementation speaks REST/JSON
/// (<see cref="RestTransport"/>), another speaks gRPC (<see cref="GrpcTransport"/>); the ADO layer is
/// transport-agnostic and works entirely in CamusDB domain types (<see cref="CamusResultSet"/>,
/// <see cref="ColumnValue"/>, <see cref="CamusBranchRow"/>) regardless of which is chosen.
///
/// <para>Every method performs exactly one round-trip and translates protocol-level failures into
/// <see cref="CamusException"/> (carrying the server's <c>CADBxxxx</c> code). Protocol-agnostic retry
/// policies — the CADB0509 finalize loop, the transient create-database loop — stay in the ADO layer, so
/// each transport call is a single attempt.</para>
/// </summary>
internal interface ICamusTransport
{
    /// <summary>The protocol this transport implements. Used mainly by tests and diagnostics.</summary>
    CamusProtocol Protocol { get; }

    /// <summary>Begins an explicit transaction and returns the server-minted handle.</summary>
    Task<StartTransactionResult> StartTransactionAsync(
        string endpoint, string database, CamusTransactionOptions options, int timeoutSeconds, CancellationToken cancellationToken);

    /// <summary>Commits (<paramref name="commit"/> = true) or rolls back the transaction identified by the
    /// handle. A single attempt — the caller owns the CADB0509 finalize-retry loop. <paramref name="streamSlot"/>
    /// is the slot returned by <see cref="StartTransactionAsync"/> (gRPC pins the finalize to the same
    /// stream as the transaction's statements); REST ignores it.</summary>
    Task FinalizeTransactionAsync(
        bool commit, string endpoint, string database, long txnIdPT, uint txnIdCounter, int? streamSlot, int timeoutSeconds, CancellationToken cancellationToken);

    /// <summary>Runs a <c>SELECT</c> and returns its decoded result set (schema + rows) plus cache metadata.</summary>
    Task<QueryTransportResult> ExecuteQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken);

    /// <summary>Runs an <c>INSERT</c>/<c>UPDATE</c>/<c>DELETE</c> and returns the affected-row count.</summary>
    Task<int> ExecuteNonQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken);

    /// <summary>Runs a DDL statement (<c>CREATE</c>/<c>ALTER</c>/<c>DROP TABLE</c>, indexes). Returns success.</summary>
    Task<bool> ExecuteDdlAsync(TransportSqlRequest request, CancellationToken cancellationToken);

    /// <summary>Liveness check. Returns true when the server answers.</summary>
    Task<bool> PingAsync(string endpoint, int timeoutSeconds, CancellationToken cancellationToken);

    /// <summary>Creates a database. A single attempt — the caller owns the transient-retry loop.</summary>
    Task CreateDatabaseAsync(
        string endpoint, string database, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken);

    /// <summary>Creates a copy-on-write branch database. A single attempt — the caller owns the retry loop.</summary>
    Task CreateBranchDatabaseAsync(
        string endpoint, string branchName, string sourceDatabaseName, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken);

    /// <summary>Drops a database.</summary>
    Task DropDatabaseAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken);

    /// <summary>Returns every transitive descendant of the database (SHOW BRANCHES semantics).</summary>
    Task<IReadOnlyList<CamusBranchRow>> ShowBranchesAsync(
        string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken);

    /// <summary>Returns the full ancestry chain of the database (SHOW ANCESTORS semantics).</summary>
    Task<IReadOnlyList<CamusBranchRow>> ShowAncestorsAsync(
        string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken);
}
