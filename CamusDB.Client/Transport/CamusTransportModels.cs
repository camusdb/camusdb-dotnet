
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Transport;

/// <summary>
/// The wire protocol a <see cref="CamusConnection"/> speaks to the server. Selected per connection via
/// the <c>Protocol=</c> connection-string key; defaults to <see cref="Rest"/> when the key is absent or
/// unrecognized.
/// </summary>
public enum CamusProtocol
{
    /// <summary>The REST/JSON HTTP API (default). Talks to the <c>execute-sql-*</c> endpoints.</summary>
    Rest = 0,

    /// <summary>The gRPC API (<c>CamusSql</c> service). Requires the endpoint to address the gRPC port.</summary>
    Grpc = 1,
}

/// <summary>
/// A protocol-neutral SQL request handed to an <see cref="ICamusTransport"/>. Carries everything the
/// transport needs to run one <c>SELECT</c>/<c>DML</c>/<c>DDL</c> statement: the resolved endpoint, the
/// database, the SQL text, bound parameters, and either the explicit transaction handle to join or the
/// autocommit concurrency options to begin the statement's own short transaction with.
/// </summary>
internal sealed class TransportSqlRequest
{
    public required string Endpoint { get; init; }

    public required string Database { get; init; }

    public required string Sql { get; init; }

    public IReadOnlyDictionary<string, ColumnValue>? Parameters { get; init; }

    /// <summary>Explicit transaction to resume. When set (together with <see cref="TxnIdCounter"/>) the
    /// statement joins that transaction and <see cref="AutocommitOptions"/> is ignored.</summary>
    public long? TxnIdPT { get; init; }

    public uint? TxnIdCounter { get; init; }

    /// <summary>Concurrency options for the autocommit transaction begun for this statement, applied only
    /// when there is no explicit transaction. Null on the read-query path (no locking mode).</summary>
    public CamusTransactionOptions? AutocommitOptions { get; init; }

    /// <summary>Opaque transport routing hint. For the gRPC batching transport this is the reserved
    /// <c>BatchExecute</c> stream slot a transaction's ops pin to (so the server orders them); the REST
    /// transport ignores it. Null for autocommit statements (the transport picks a stream freely).</summary>
    public int? StreamSlot { get; init; }

    public int TimeoutSeconds { get; init; }

    public bool HasTransaction => TxnIdPT.HasValue && TxnIdCounter.HasValue;
}

/// <summary>Identity of a transaction the server minted for <see cref="ICamusTransport.StartTransactionAsync"/>,
/// plus the opaque stream slot (gRPC batching) the transaction's later ops must pin to. <see cref="StreamSlot"/>
/// is null for transports that don't pin (REST).</summary>
internal readonly struct StartTransactionResult(long txnIdPT, uint txnIdCounter, int? streamSlot = null)
{
    public long TxnIdPT { get; } = txnIdPT;

    public uint TxnIdCounter { get; } = txnIdCounter;

    public int? StreamSlot { get; } = streamSlot;
}

/// <summary>The decoded result of a query plus any server-reported cache metadata (REST only; gRPC has
/// no cache-metadata channel and leaves it null).</summary>
internal sealed class QueryTransportResult(CamusResultSet resultSet, CamusCacheMetadata? cacheMetadata)
{
    public CamusResultSet ResultSet { get; } = resultSet;

    public CamusCacheMetadata? CacheMetadata { get; } = cacheMetadata;
}
