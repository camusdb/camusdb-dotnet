/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using CamusDB.Client.Transport;

namespace CamusDB.Client.Tests;

/// <summary>
/// The driver's spans and the <c>db.client.operation.duration</c> histogram, against a fake transport.
/// Other test classes run at the same time and emit on the same sources, so each test uses its own
/// database name and looks only at the data for that name.
/// </summary>
public sealed class TestTelemetry : IDisposable
{
    private const string Endpoint = "http://db1.telemetry.test:5095";

    private readonly string database = "telemetry_" + Guid.NewGuid().ToString("N");

    private readonly ConcurrentQueue<Activity> stopped = new();

    private readonly ConcurrentQueue<(double Value, Dictionary<string, object?> Tags)> measurements = new();

    private readonly ActivityListener activityListener;

    private readonly MeterListener meterListener;

    public TestTelemetry()
    {
        activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == CamusTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.GetTagItem("db.namespace") as string == database)
                    stopped.Enqueue(activity);
            },
        };
        ActivitySource.AddActivityListener(activityListener);

        meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == CamusTelemetry.MeterName && instrument.Name == "db.client.operation.duration")
                    listener.EnableMeasurementEvents(instrument);
            },
        };
        meterListener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            Dictionary<string, object?> copy = new();
            foreach (KeyValuePair<string, object?> tag in tags)
                copy[tag.Key] = tag.Value;

            if (copy.GetValueOrDefault("db.namespace") as string == database)
                measurements.Enqueue((value, copy));
        });
        meterListener.Start();
    }

    public void Dispose()
    {
        activityListener.Dispose();
        meterListener.Dispose();
    }

    private (CamusConnection Connection, FakeTransport Transport) Connect(string extra = "")
    {
        CamusConnectionStringBuilder builder = new($"Endpoint={Endpoint};Database={database};MaxAutoPrepare=0;{extra}");
        FakeTransport transport = new();
        builder.UseTransport(transport);

        CamusConnection connection = new(builder);
        connection.Open();
        return (connection, transport);
    }

    private Activity Single(string name) => Assert.Single(stopped, a => a.DisplayName == name);

    [Fact]
    public async Task Query_RecordsAClientSpanWithTheSemanticConventionAttributes()
    {
        (CamusConnection connection, _) = Connect();

        using CamusCommand command = connection.CreateCamusCommand("  select * from robots where id = @id");
        command.Parameters.Add("@id", ColumnType.Integer64, 5);
        await using (CamusDataReader reader = await command.ExecuteReaderAsync())
        {
        }

        Activity span = Single("SELECT robots");
        Assert.Equal(ActivityKind.Client, span.Kind);
        Assert.Equal("robots", span.GetTagItem("db.collection.name"));
        Assert.Equal("SELECT robots", span.GetTagItem("db.query.summary"));
        Assert.Equal(ActivityStatusCode.Unset, span.Status);
        Assert.Equal("camusdb", span.GetTagItem("db.system.name"));
        Assert.Equal(database, span.GetTagItem("db.namespace"));
        Assert.Equal("SELECT", span.GetTagItem("db.operation.name"));
        Assert.Equal("db1.telemetry.test", span.GetTagItem("server.address"));
        Assert.Equal(5095, span.GetTagItem("server.port"));
        Assert.Equal("rest", span.GetTagItem("camusdb.transport"));

        // The SQL text is opt-in, and parameter values are never recorded.
        Assert.Null(span.GetTagItem("db.query.text"));
        Assert.DoesNotContain(span.TagObjects, tag => tag.Key.StartsWith("db.query.parameter", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TraceQueryText_AddsTheSqlTextToTheSpan()
    {
        (CamusConnection connection, _) = Connect("TraceQueryText=true");

        using CamusCommand command = connection.CreateCamusCommand("SELECT * FROM robots");
        await using (await command.ExecuteReaderAsync())
        {
        }

        Assert.Equal("SELECT * FROM robots", Single("SELECT robots").GetTagItem("db.query.text"));
    }

    [Fact]
    public async Task Failure_SetsErrorStatusAndTheServerCode_OnTheSpanAndTheMetric()
    {
        (CamusConnection connection, FakeTransport transport) = Connect();
        transport.NonQueryFailure = new CamusException("CADB0503", "conflict");

        using CamusCommand command = connection.CreateCamusCommand("UPDATE robots SET name = 'x'");
        CamusException thrown = await Assert.ThrowsAsync<CamusException>(() => command.ExecuteNonQueryAsync());
        Assert.Equal("CADB0503", thrown.Code);

        Activity span = Single("UPDATE robots");
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("CADB0503", span.GetTagItem("error.type"));
        Assert.Equal("CADB0503", span.GetTagItem("db.response.status_code"));

        (double _, Dictionary<string, object?> tags) = Assert.Single(measurements);
        Assert.Equal("UPDATE", tags["db.operation.name"]);
        Assert.Equal("CADB0503", tags["error.type"]);
    }

    [Fact]
    public async Task Duration_IsRecordedOncePerOperation_WithLowCardinalityTags()
    {
        (CamusConnection connection, _) = Connect("TraceQueryText=true");

        using CamusCommand command = connection.CreateCamusCommand("DELETE FROM robots WHERE id = 1");
        Assert.Equal(3, await command.ExecuteNonQueryAsync());

        (double value, Dictionary<string, object?> tags) = Assert.Single(measurements);
        Assert.True(value >= 0);
        Assert.Equal("camusdb", tags["db.system.name"]);
        Assert.Equal("DELETE", tags["db.operation.name"]);
        Assert.Equal("db1.telemetry.test", tags["server.address"]);
        Assert.Equal(5095, tags["server.port"]);
        Assert.False(tags.ContainsKey("error.type"));

        // The SQL text never goes on the metric, even with TraceQueryText=true.
        Assert.False(tags.ContainsKey("db.query.text"));
    }

    [Fact]
    public async Task StreamReader_EndsTheSpanAtTheLastRow_NotWhenTheCallReturns()
    {
        (CamusConnection connection, _) = Connect();

        using CamusCommand command = connection.CreateCamusCommand("SELECT name FROM robots");
        await using CamusDataReader reader = await command.ExecuteStreamReaderAsync();

        Assert.Empty(stopped);

        Assert.True(await reader.ReadAsync());
        Assert.True(await reader.ReadAsync());
        Assert.Empty(stopped);

        Assert.False(await reader.ReadAsync());
        Single("SELECT robots");

        // A later close does not end the operation a second time.
        await reader.DisposeAsync();
        Assert.Single(stopped);
        Assert.Single(measurements);
    }

    [Fact]
    public async Task StreamReader_ClosedEarly_EndsTheSpanAsASuccess()
    {
        (CamusConnection connection, _) = Connect();

        using CamusCommand command = connection.CreateCamusCommand("SELECT name FROM robots");
        CamusDataReader reader = await command.ExecuteStreamReaderAsync();
        Assert.True(await reader.ReadAsync());

        await reader.DisposeAsync();

        Assert.Equal(ActivityStatusCode.Unset, Single("SELECT robots").Status);
    }

    [Fact]
    public async Task StreamReader_ReadFailure_EndsTheSpanAsAFailure()
    {
        (CamusConnection connection, FakeTransport transport) = Connect();
        transport.StreamFailure = new CamusException("CADB0503", "late conflict");

        using CamusCommand command = connection.CreateCamusCommand("SELECT name FROM robots");
        await using CamusDataReader reader = await command.ExecuteStreamReaderAsync();

        await Assert.ThrowsAsync<CamusException>(async () => await reader.ReadAsync());

        Activity span = Single("SELECT robots");
        Assert.Equal(ActivityStatusCode.Error, span.Status);
        Assert.Equal("CADB0503", span.GetTagItem("error.type"));
    }

    [Fact]
    public async Task StreamReader_EndedInALaterCall_KeepsTheCallersCurrentActivity()
    {
        (CamusConnection connection, _) = Connect();

        using CamusCommand command = connection.CreateCamusCommand("SELECT name FROM robots");
        CamusDataReader reader = await command.ExecuteStreamReaderAsync();

        using Activity outer = new Activity("application-work").Start();
        await reader.DisposeAsync();

        Assert.Same(outer, Activity.Current);
    }

    [Fact]
    public async Task Span_IsAChildOfTheCallersActivity()
    {
        (CamusConnection connection, _) = Connect();

        using Activity parent = new Activity("request").Start();
        using CamusCommand command = connection.CreateCamusCommand("SELECT 1");
        await using (await command.ExecuteReaderAsync())
        {
        }

        Activity span = Single($"SELECT {database}");
        Assert.Equal(parent.TraceId, span.TraceId);
        Assert.Equal(parent.SpanId, span.ParentSpanId);
        Assert.Same(parent, Activity.Current);
    }

    [Fact]
    public async Task Transaction_RecordsBeginStatementAndCommit()
    {
        (CamusConnection connection, _) = Connect();

        await using CamusTransaction transaction = await connection.BeginTransactionAsync();
        using CamusCommand command = connection.CreateCamusCommand("INSERT INTO robots (id) VALUES (1)");
        command.Transaction = transaction;
        await command.ExecuteNonQueryAsync();
        await transaction.CommitAsync();

        Assert.Equal(
            [$"BEGIN {database}", "INSERT robots", $"COMMIT {database}"],
            stopped.Select(a => a.DisplayName));
        Assert.Equal(3, measurements.Count);
    }

    [Fact]
    public async Task Rollback_IsRecorded()
    {
        (CamusConnection connection, _) = Connect();

        await using CamusTransaction transaction = await connection.BeginTransactionAsync();
        await transaction.RollbackAsync();

        Assert.Equal("ROLLBACK", Single($"ROLLBACK {database}").GetTagItem("db.operation.name"));
    }

    [Fact]
    public async Task Ddl_IsRecorded()
    {
        (CamusConnection connection, _) = Connect();

        using CamusCommand command = connection.CreateCamusCommand("CREATE TABLE robots (id OID PRIMARY KEY NOT NULL)");
        await command.ExecuteNonQueryAsync();

        Single("CREATE robots");
    }

    [Fact]
    public async Task TypedInsert_NamesTheTableAsTheCollection()
    {
        (CamusConnection connection, _) = Connect("TraceQueryText=true");

        using CamusInsertCommand command = connection.CreateInsertCommand("robots");
        command.Parameters.Add("id", ColumnType.Integer64, 1);
        await command.ExecuteNonQueryAsync();

        Activity span = Single("INSERT robots");
        Assert.Equal("robots", span.GetTagItem("db.collection.name"));

        // The command text of a typed insert is a table name, not SQL.
        Assert.Null(span.GetTagItem("db.query.text"));
    }

    [Fact]
    public async Task TraceQueryTextSanitized_RecordsTheSqlTextWithoutItsLiteralValues()
    {
        (CamusConnection connection, _) = Connect("TraceQueryText=Sanitized");

        using CamusCommand command = connection.CreateCamusCommand(
            "SELECT `o`.`id` FROM `orders` AS `o` JOIN customers c ON c.id = `o`.`customer_id` "
            + "WHERE `o`.`email` = 'player@example.com' AND `o`.`total` > 10.5 AND `o`.`region` = @region");
        command.Parameters.Add("@region", ColumnType.String, "PH");
        await using (await command.ExecuteReaderAsync())
        {
        }

        Activity span = Single("SELECT orders customers");
        Assert.Equal(
            "SELECT `o`.`id` FROM `orders` AS `o` JOIN customers c ON c.id = `o`.`customer_id` "
            + "WHERE `o`.`email` = ? AND `o`.`total` > ? AND `o`.`region` = @region",
            span.GetTagItem("db.query.text"));

        // Two tables: a summary, but no single collection.
        Assert.Equal("SELECT orders customers", span.GetTagItem("db.query.summary"));
        Assert.Null(span.GetTagItem("db.collection.name"));
    }

    [Fact]
    public async Task AStatementThatNamesNoTable_KeepsTheDatabaseAsItsTarget()
    {
        (CamusConnection connection, _) = Connect("TraceQueryText=Sanitized");

        using CamusCommand command = connection.CreateCamusCommand("SELECT 1");
        await using (await command.ExecuteReaderAsync())
        {
        }

        Activity span = Single($"SELECT {database}");
        Assert.Null(span.GetTagItem("db.query.summary"));
        Assert.Null(span.GetTagItem("db.collection.name"));
        Assert.Equal("SELECT ?", span.GetTagItem("db.query.text"));
    }

    [Theory]
    [InlineData("TraceQueryText=false", CamusQueryTextMode.Off)]
    [InlineData("TraceQueryText=true", CamusQueryTextMode.Raw)]
    [InlineData("TraceQueryText=Sanitized", CamusQueryTextMode.Sanitized)]
    [InlineData("TraceQueryText=sanitized", CamusQueryTextMode.Sanitized)]
    [InlineData("TraceQueryText=yes", CamusQueryTextMode.Off)]
    [InlineData("", CamusQueryTextMode.Off)]
    public void TraceQueryText_ReadsItsMode(string option, CamusQueryTextMode expected)
    {
        CamusConnectionStringBuilder builder = new($"Endpoint={Endpoint};Database={database};{option}");

        Assert.Equal(expected, builder.TraceQueryTextMode);
        Assert.Equal(expected != CamusQueryTextMode.Off, builder.TraceQueryText);
    }

    [Fact]
    public async Task Pipeline_RecordsOneBatchSpan()
    {
        (CamusConnection connection, _) = Connect();

        await using CamusTransaction transaction = await connection.BeginTransactionAsync();
        CamusPipeline pipeline = transaction.CreatePipeline();

        using CamusCommand first = connection.CreateCamusCommand("UPDATE robots SET name = 'a'");
        using CamusCommand second = connection.CreateCamusCommand("UPDATE robots SET name = 'b'");
        Task<int> one = pipeline.QueueNonQuery(first);
        Task<int> two = pipeline.QueueNonQuery(second);
        await pipeline.CommitAsync();
        await Task.WhenAll(one, two);

        Activity span = Single($"BATCH {database}");
        Assert.Equal(2, span.GetTagItem("db.operation.batch.size"));
        Assert.Equal(true, span.GetTagItem("camusdb.pipeline.commit"));
    }

    [Theory]
    [InlineData("SELECT 1", "SELECT")]
    [InlineData("  \n\tselect 1", "SELECT")]
    [InlineData("((SELECT 1))", "SELECT")]
    [InlineData("insert into t values (1)", "INSERT")]
    [InlineData("TRUNCATE t", "TRUNCATE")]
    [InlineData("CALL transfer(@a, @b)", "CALL")]
    [InlineData("WITH x AS (SELECT 1) SELECT * FROM x", null)]
    [InlineData("frobnicate", null)]
    [InlineData("", null)]
    [InlineData(null, null)]
    public void OperationName_IsTheFirstKnownKeyword(string? sql, string? expected)
    {
        Assert.Equal(expected, CamusOperation.OperationName(sql));
    }

    /// <summary>Answers every call with a fixed result, or with the configured failure.</summary>
    internal sealed class FakeTransport : ICamusTransport
    {
        public CamusException? NonQueryFailure { get; set; }

        /// <summary>How many non-query calls fail with <see cref="NonQueryFailure"/> before one succeeds.</summary>
        public int NonQueryFailuresLeft { get; set; } = int.MaxValue;

        public CamusException? StreamFailure { get; set; }

        /// <summary>How many finalize calls fail with <c>CADB0509</c> before one succeeds.</summary>
        public int FinalizeFailures { get; set; }

        public Exception? PrepareFailure { get; set; }

        public int PrepareCalls => Volatile.Read(ref prepareCalls);

        private int prepareCalls;

        public CamusProtocol Protocol => CamusProtocol.Rest;

        private static CamusResultSet Rows() => CamusResultSet.FromRows(
        [
            new Dictionary<string, ColumnValue> { ["name"] = new() { Type = ColumnType.String, StrValue = "a" } },
            new Dictionary<string, ColumnValue> { ["name"] = new() { Type = ColumnType.String, StrValue = "b" } },
        ]);

        public Task<StartTransactionResult> StartTransactionAsync(string endpoint, string database, CamusTransactionOptions options, int timeoutSeconds, CancellationToken cancellationToken)
            => Task.FromResult(new StartTransactionResult(1, 1));

        public Task FinalizeTransactionAsync(bool commit, string endpoint, string database, long txnIdPT, uint txnIdCounter, int? streamSlot, int timeoutSeconds, CancellationToken cancellationToken)
        {
            if (FinalizeFailures <= 0)
                return Task.CompletedTask;

            FinalizeFailures--;
            return Task.FromException(new CamusException("CADB0509", "finalize outcome unresolved"));
        }

        public Task<QueryTransportResult> ExecuteQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new QueryTransportResult(Rows(), null));

        public Task<CamusRowSource> ExecuteQueryStreamAsync(TransportSqlRequest request, CancellationToken cancellationToken)
            => Task.FromResult(StreamFailure is { } failure
                ? new FailingRowSource(failure)
                : CamusRowSource.Buffered(Rows()));

        public Task<NonQueryTransportResult> ExecuteNonQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
        {
            if (NonQueryFailure is { } failure && NonQueryFailuresLeft > 0)
            {
                NonQueryFailuresLeft--;
                return Task.FromException<NonQueryTransportResult>(failure);
            }

            return Task.FromResult(new NonQueryTransportResult(3));
        }

        public Task<PipelineTransportResult> ExecutePipelineAsync(TransportPipelineRequest request, CancellationToken cancellationToken)
            => SequentialPipeline.RunAsync(this, request, cancellationToken);

        public Task<int> InsertAsync(TransportInsertRequest request, CancellationToken cancellationToken)
            => Task.FromResult(1);

        public Task<bool> ExecuteDdlAsync(TransportSqlRequest request, CancellationToken cancellationToken)
            => Task.FromResult(true);

        public Task<PreparedStatementInfo> PrepareAsync(string endpoint, string database, string sql, int timeoutSeconds, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref prepareCalls);

            return PrepareFailure is { } failure
                ? Task.FromException<PreparedStatementInfo>(failure)
                : Task.FromResult(new PreparedStatementInfo([]));
        }

        public Task ClosePreparedAsync(string endpoint, string database, string sql, CancellationToken cancellationToken)
            => Task.CompletedTask;

        public Task<bool> PingAsync(string endpoint, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<CamusNodeStats> GetNodeStatsAsync(string endpoint, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task CreateDatabaseAsync(string endpoint, string database, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task CreateBranchDatabaseAsync(string endpoint, string branchName, string sourceDatabaseName, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task DropDatabaseAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<CamusBranchRow>> ShowBranchesAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<CamusBranchRow>> ShowAncestorsAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();
    }

    /// <summary>A stream whose first read fails, as a late conflict does.</summary>
    private sealed class FailingRowSource(CamusException failure) : CamusRowSource
    {
        public override string[] ColumnNames => ["name"];

        public override ColumnType[]? ColumnTypes => [ColumnType.String];

        public override bool HasRows => true;

        public override bool Read() => throw failure;

        public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken) => ValueTask.FromException<bool>(failure);

        public override ref readonly ColumnValue GetCell(int ordinal) => throw new InvalidOperationException();
    }
}
