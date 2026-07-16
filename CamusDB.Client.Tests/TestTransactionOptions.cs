/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Text.Json;

namespace CamusDB.Client.Tests;

/// <summary>
/// Deterministic (server-free) coverage of the transaction-options plumbing: connection-string parsing,
/// option merge precedence, wire-string mapping, request-DTO serialization, and the ADO.NET isolation
/// mapping. Server-backed optimistic-conflict behavior is exercised separately (needs a live node).
/// </summary>
public class TestTransactionOptions
{
    // ---- Connection-string defaults ---------------------------------------------------------------

    [Fact]
    public void ConnectionStringParsesAllThreeDefaults()
    {
        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:5095;Database=test;Locking=Optimistic;IsolationLevel=ReadCommitted;TransactionMode=ReadOnly");

        CamusTransactionOptions options = builder.DefaultTransactionOptions;

        Assert.Equal(CamusLocking.Optimistic, options.Locking);
        Assert.Equal(CamusIsolationLevel.ReadCommitted, options.IsolationLevel);
        Assert.Equal(CamusTransactionMode.ReadOnly, options.Mode);
    }

    [Fact]
    public void ConnectionStringDefaultsAreCaseInsensitive()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=e;Database=test;Locking=optimistic");

        Assert.Equal(CamusLocking.Optimistic, builder.DefaultTransactionOptions.Locking);
    }

    [Fact]
    public void ConnectionStringMissingOrGarbageLeavesKnobsNull()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=e;Database=test;Locking=banana");

        CamusTransactionOptions options = builder.DefaultTransactionOptions;

        Assert.Null(options.Locking);
        Assert.Null(options.IsolationLevel);
        Assert.Null(options.Mode);
    }

    // ---- Merge precedence --------------------------------------------------------------------------

    [Fact]
    public void WithDefaultsKeepsOwnNonNullKnobsAndFillsNullsFromFallback()
    {
        CamusTransactionOptions requested = new() { Locking = CamusLocking.Optimistic };
        CamusTransactionOptions fallback = new()
        {
            Locking = CamusLocking.Pessimistic,
            IsolationLevel = CamusIsolationLevel.ReadCommitted,
        };

        CamusTransactionOptions merged = requested.WithDefaults(fallback);

        Assert.Equal(CamusLocking.Optimistic, merged.Locking);            // own value wins
        Assert.Equal(CamusIsolationLevel.ReadCommitted, merged.IsolationLevel); // filled from fallback
        Assert.Null(merged.Mode);
    }

    [Fact]
    public void ResolveTransactionOptionsPrecedenceExplicitThenConnectionThenConnString()
    {
        CamusConnectionStringBuilder builder = new(
            "Endpoint=e;Database=test;Locking=Pessimistic;IsolationLevel=Serializable");
        using CamusConnection connection = new(builder)
        {
            // connection-level default (as the EF provider would set from UseOptimisticLocking)
            DefaultTransactionOptions = new CamusTransactionOptions { Locking = CamusLocking.Optimistic },
        };

        // No explicit request: connection default (Optimistic) wins over conn-string (Pessimistic);
        // isolation is unset on the connection default, so it falls through to the conn-string value.
        CamusTransactionOptions resolved = connection.ResolveTransactionOptions(null);
        Assert.Equal(CamusLocking.Optimistic, resolved.Locking);
        Assert.Equal(CamusIsolationLevel.Serializable, resolved.IsolationLevel);

        // Explicit request overrides everything it specifies.
        CamusTransactionOptions explicitResolved = connection.ResolveTransactionOptions(
            new CamusTransactionOptions { Locking = CamusLocking.Pessimistic });
        Assert.Equal(CamusLocking.Pessimistic, explicitResolved.Locking);
    }

    // ---- Wire strings ------------------------------------------------------------------------------

    [Fact]
    public void WireStringsMatchServerVocabulary()
    {
        CamusTransactionOptions options = new()
        {
            IsolationLevel = CamusIsolationLevel.ReadCommitted,
            Mode = CamusTransactionMode.ReadOnly,
            Locking = CamusLocking.Optimistic,
        };

        Assert.Equal("ReadCommitted", options.IsolationLevelWire);
        Assert.Equal("ReadOnly", options.ModeWire);
        Assert.Equal("Optimistic", options.LockingWire);
    }

    [Fact]
    public void UnsetKnobsHaveNullWireStrings()
    {
        CamusTransactionOptions options = CamusTransactionOptions.Default;

        Assert.Null(options.IsolationLevelWire);
        Assert.Null(options.ModeWire);
        Assert.Null(options.LockingWire);
    }

    // ---- Request DTO serialization -----------------------------------------------------------------

    [Fact]
    public void StartTransactionRequestOmitsUnsetKnobs()
    {
        CamusStartTransactionRequest request = new() { DatabaseName = "test" };

        string json = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusStartTransactionRequest);

        Assert.DoesNotContain("isolationLevel", json);
        Assert.DoesNotContain("transactionMode", json);
        Assert.DoesNotContain("locking", json);
    }

    [Fact]
    public void StartTransactionRequestEmitsSetKnobsWithCamelCaseNames()
    {
        CamusStartTransactionRequest request = new()
        {
            DatabaseName = "test",
            IsolationLevel = "ReadCommitted",
            TransactionMode = "ReadOnly",
            Locking = "Optimistic",
        };

        string json = JsonSerializer.Serialize(request, CamusJsonSerializerContext.Default.CamusStartTransactionRequest);

        Assert.Contains("\"isolationLevel\":\"ReadCommitted\"", json);
        Assert.Contains("\"transactionMode\":\"ReadOnly\"", json);
        Assert.Contains("\"locking\":\"Optimistic\"", json);
    }

    [Fact]
    public void NonQueryAndDdlRequestsOmitUnsetKnobs()
    {
        string nonQuery = JsonSerializer.Serialize(
            new CamusExecuteSqlNonQueryRequest { DatabaseName = "test", Sql = "UPDATE t SET x=1" },
            CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryRequest);
        string ddl = JsonSerializer.Serialize(
            new CamusExecuteDDLRequest { DatabaseName = "test", Sql = "CREATE TABLE t(id INT)" },
            CamusJsonSerializerContext.Default.CamusExecuteDDLRequest);

        Assert.DoesNotContain("locking", nonQuery);
        Assert.DoesNotContain("locking", ddl);
    }

    [Fact]
    public void NonQueryRequestEmitsLockingWhenSet()
    {
        string json = JsonSerializer.Serialize(
            new CamusExecuteSqlNonQueryRequest { DatabaseName = "test", Sql = "UPDATE t SET x=1", Locking = "Optimistic" },
            CamusJsonSerializerContext.Default.CamusExecuteSqlNonQueryRequest);

        Assert.Contains("\"locking\":\"Optimistic\"", json);
    }

    // ---- ADO.NET isolation mapping -----------------------------------------------------------------

    [Fact]
    public void CamusTransactionReportsMappedIsolationLevel()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=e;Database=test");
        using CamusConnection connection = new(builder);

        CamusTransaction readCommitted = new(1, 0, "e", connection, builder,
            new CamusTransactionOptions { IsolationLevel = CamusIsolationLevel.ReadCommitted });
        CamusTransaction serializable = new(1, 0, "e", connection, builder,
            new CamusTransactionOptions { IsolationLevel = CamusIsolationLevel.Serializable });
        CamusTransaction unspecified = new(1, 0, "e", connection, builder, CamusTransactionOptions.Default);

        Assert.Equal(IsolationLevel.ReadCommitted, readCommitted.IsolationLevel);
        Assert.Equal(IsolationLevel.Serializable, serializable.IsolationLevel);
        Assert.Equal(IsolationLevel.Serializable, unspecified.IsolationLevel); // CamusDB default is Serializable
    }

    [Fact]
    public void BeginTransactionWithUnsupportedIsolationThrowsBeforeAnyNetworkCall()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=test");
        using CamusConnection connection = new(builder);

        // RepeatableRead has no CamusDB equivalent — MapIsolationLevel rejects it synchronously,
        // before an endpoint is ever contacted.
        Assert.Throws<NotSupportedException>(() => connection.BeginTransaction(IsolationLevel.RepeatableRead));
    }
}
