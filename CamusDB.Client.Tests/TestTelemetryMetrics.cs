/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using CamusDB.Client.Auth;
using CamusDB.Client.Transport;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using FakeTransport = CamusDB.Client.Tests.TestTelemetry.FakeTransport;

namespace CamusDB.Client.Tests;

/// <summary>
/// The <c>camusdb.client.*</c> metrics (retries, endpoint quarantine, prepared statements) and the
/// <c>AddCamusDBInstrumentation()</c> extensions. Each test uses its own database and endpoint names, so
/// the data of other test classes that run at the same time does not match.
/// </summary>
public sealed class TestTelemetryMetrics : IDisposable
{
    private readonly string id = Guid.NewGuid().ToString("N");

    private string Database => "metrics_" + id;

    private string Host => id + ".metrics.test";

    private string Endpoint => $"http://{Host}:5095";

    private readonly ConcurrentQueue<(string Instrument, double Value, Dictionary<string, object?> Tags)> measurements = new();

    private readonly ConcurrentQueue<Activity> stopped = new();

    private readonly MeterListener meterListener;

    private readonly ActivityListener activityListener;

    public TestTelemetryMetrics()
    {
        meterListener = new MeterListener
        {
            InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == CamusTelemetry.MeterName)
                    listener.EnableMeasurementEvents(instrument);
            },
        };
        meterListener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Capture(instrument, value, tags));
        meterListener.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Capture(instrument, value, tags));
        meterListener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Capture(instrument, value, tags));
        meterListener.Start();

        activityListener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == CamusTelemetry.ActivitySourceName,
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity =>
            {
                if (activity.GetTagItem("db.namespace") as string == Database)
                    stopped.Enqueue(activity);
            },
        };
        ActivitySource.AddActivityListener(activityListener);
    }

    public void Dispose()
    {
        meterListener.Dispose();
        activityListener.Dispose();
    }

    private void Capture(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
    {
        Dictionary<string, object?> copy = new();
        foreach (KeyValuePair<string, object?> tag in tags)
            copy[tag.Key] = tag.Value;

        measurements.Enqueue((instrument.Name, value, copy));
    }

    /// <summary>The measurements of <paramref name="instrument"/> for this test's database or host.</summary>
    private List<Dictionary<string, object?>> Of(string instrument)
        => measurements
            .Where(m => m.Instrument == instrument
                && (m.Tags.GetValueOrDefault("db.namespace") as string == Database
                    || (m.Tags.GetValueOrDefault("server.address") as string)?.EndsWith(Host, StringComparison.Ordinal) == true))
            .Select(m => m.Tags)
            .ToList();

    private (CamusConnection Connection, CamusConnectionStringBuilder Builder, FakeTransport Transport) Connect(string extra = "MaxAutoPrepare=0")
    {
        CamusConnectionStringBuilder builder = new($"Endpoint={Endpoint};Database={Database};{extra}");
        FakeTransport transport = new();
        builder.UseTransport(transport);

        CamusConnection connection = new(builder);
        connection.Open();
        return (connection, builder, transport);
    }

    private static async Task QueryAsync(CamusConnection connection, string sql)
    {
        using CamusCommand command = connection.CreateCamusCommand(sql);
        await using (await command.ExecuteReaderAsync())
        {
        }
    }

    [Fact]
    public async Task FinalizeRetry_IsCountedAndRecordedOnTheCommitSpan()
    {
        (CamusConnection connection, _, FakeTransport transport) = Connect();

        CamusTransaction transaction = await connection.BeginTransactionAsync();
        transport.FinalizeFailures = 2;
        await transaction.CommitAsync();

        List<Dictionary<string, object?>> retries = Of("camusdb.client.operation.retries");
        Assert.Equal(2, retries.Count);
        Assert.All(retries, tags =>
        {
            Assert.Equal("finalize", tags["camusdb.retry.kind"]);
            Assert.Equal("CADB0509", tags["error.type"]);
            Assert.Equal("COMMIT", tags["db.operation.name"]);
        });

        Activity commit = Assert.Single(stopped, a => a.DisplayName == $"COMMIT {Database}");
        Assert.Equal(ActivityStatusCode.Unset, commit.Status);
        Assert.Equal(2, commit.Events.Count(e => e.Name == "camusdb.retry"));
        Assert.Equal("finalize", commit.Events.First().Tags.First(t => t.Key == "camusdb.retry.kind").Value);
    }

    [Fact]
    public async Task AutocommitRetry_IsCounted()
    {
        int attempts = 0;

        await SerializableRetryHelper.ExecuteAutocommitAsync(_ =>
            ++attempts <= 2 ? Task.FromException(new CamusException("CADB0502", "lock conflict")) : Task.CompletedTask);

        Assert.Equal(3, attempts);

        // The helper knows no database, so other tests can add to the count: at least this test's two.
        Assert.True(measurements.Count(m => m.Instrument == "camusdb.client.operation.retries"
            && m.Tags["camusdb.retry.kind"] as string == "autocommit"
            && m.Tags["error.type"] as string == "CADB0502") >= 2);
    }

    [Fact]
    public async Task TokenRenewal_IsCountedAndRecordedOnTheCurrentDriverSpan()
    {
        (_, CamusConnectionStringBuilder builder, FakeTransport inner) = Connect();
        inner.NonQueryFailure = new CamusException("CADB0516", "token rejected");
        inner.NonQueryFailuresLeft = 1;

        CamusTokenProvider provider = new(
            CamusCredentials.FromPassword("admin", "secret"), () => new FakeLoginClient(), () => Endpoint, () => 10, null, null);
        await provider.GetTokenAsync(default);
        AuthenticatingTransport transport = new(inner, provider);

        CamusOperation operation = CamusOperation.Start(builder, "UPDATE")!;
        NonQueryTransportResult result = await transport.ExecuteNonQueryAsync(
            new TransportSqlRequest { Endpoint = Endpoint, Database = Database, Sql = "UPDATE t SET a = 1", TimeoutSeconds = 10 }, default);
        operation.Complete();

        Assert.Equal(3, result.AffectedRows);

        Activity span = Assert.Single(stopped);
        ActivityEvent retry = Assert.Single(span.Events, e => e.Name == "camusdb.retry");
        Assert.Equal("token_renewal", retry.Tags.First(t => t.Key == "camusdb.retry.kind").Value);
        Assert.Equal("CADB0516", retry.Tags.First(t => t.Key == "error.type").Value);

        Assert.Contains(measurements, m => m.Instrument == "camusdb.client.operation.retries"
            && m.Tags["camusdb.retry.kind"] as string == "token_renewal");
    }

    [Fact]
    public async Task BlockingLogin_IsRecordedOnTheStatementSpanThatWaitedForIt()
    {
        (_, CamusConnectionStringBuilder builder, _) = Connect();

        // No token yet: the statement waits for the login, and its span carries the time. The token is asked
        // for inside the operation, as the REST and gRPC transports ask for it before they send the statement.
        CamusTokenProvider provider = new(
            CamusCredentials.FromPassword("admin", "secret"), () => new FakeLoginClient(), () => Endpoint, () => 10, null, null);

        CamusOperation operation = CamusOperation.Start(builder, "UPDATE")!;
        Assert.Equal("token-1", await provider.GetTokenAsync(default));
        operation.Complete();

        Activity span = Assert.Single(stopped);
        ActivityEvent login = Assert.Single(span.Events, e => e.Name == "camusdb.login");
        Assert.Equal("blocking", login.Tags.First(t => t.Key == "camusdb.login.mode").Value);
    }

    [Fact]
    public void Quarantine_IsCounted_AndTheGaugeShowsWhichEndpointIsSetAside()
    {
        string other = $"http://other.{Host}:5095";
        CamusConnectionStringBuilder builder = new($"Endpoint={Endpoint},{other};Database={Database}");

        builder.MarkEndpointUnreachable(Endpoint);
        builder.MarkEndpointUnreachable("http://not-in-the-pool.test:5095");

        Dictionary<string, object?> quarantine = Assert.Single(Of("camusdb.client.endpoint.quarantines"));
        Assert.Equal(Host, quarantine["server.address"]);
        Assert.Equal(5095, quarantine["server.port"]);

        meterListener.RecordObservableInstruments();

        Dictionary<string, double> gauge = measurements
            .Where(m => m.Instrument == "camusdb.client.endpoint.quarantined")
            .Where(m => (m.Tags["server.address"] as string)!.EndsWith(Host, StringComparison.Ordinal))
            .ToDictionary(m => (string)m.Tags["server.address"]!, m => m.Value);

        Assert.Equal(1, gauge[Host]);
        Assert.Equal(0, gauge["other." + Host]);
    }

    [Fact]
    public async Task PreparedStatements_CountHitsRegistrationsEvictionsAndThePreparedTotal()
    {
        (CamusConnection connection, _, FakeTransport transport) = Connect("MaxAutoPrepare=1;AutoPrepareMinUsages=1");

        await QueryAsync(connection, "SELECT 1");   // registered
        await QueryAsync(connection, "SELECT 1");   // hit
        await QueryAsync(connection, "SELECT 2");   // evicts SELECT 1, registered

        Assert.Equal(2, transport.PrepareCalls);
        Assert.Equal(
            ["registered", "hit", "registered"],
            Of("camusdb.client.prepared_statement.lookups").Select(t => t["camusdb.prepared_statement.result"]));
        Assert.Equal(
            ["success", "success"],
            Of("camusdb.client.prepared_statement.registrations").Select(t => t["camusdb.prepared_statement.result"]));
        Assert.Single(Of("camusdb.client.prepared_statement.evictions"));

        meterListener.RecordObservableInstruments();
        (_, double count, _) = Assert.Single(measurements,
            m => m.Instrument == "camusdb.client.prepared_statement.count" && m.Tags["db.namespace"] as string == Database);
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task PreparedStatements_ARefusedRegistrationIsCountedAndTheStatementRunsInline()
    {
        (CamusConnection connection, _, FakeTransport transport) = Connect("MaxAutoPrepare=8;AutoPrepareMinUsages=1");
        transport.PrepareFailure = new CamusException("CADB0001", "cannot prepare");

        await QueryAsync(connection, "SELECT 1");
        await QueryAsync(connection, "SELECT 1");

        // The refusal is remembered: the second execution does not ask again.
        Assert.Equal(1, transport.PrepareCalls);

        Dictionary<string, object?> registration = Assert.Single(Of("camusdb.client.prepared_statement.registrations"));
        Assert.Equal("refused", registration["camusdb.prepared_statement.result"]);
        Assert.Equal("CADB0001", registration["error.type"]);
        Assert.Equal(
            ["miss", "miss"],
            Of("camusdb.client.prepared_statement.lookups").Select(t => t["camusdb.prepared_statement.result"]));
    }

    [Fact]
    public async Task PreparedStatements_AServerWithoutSupportIsCountedAsUnsupported()
    {
        (CamusConnection connection, _, FakeTransport transport) = Connect("MaxAutoPrepare=8;AutoPrepareMinUsages=1");
        transport.PrepareFailure = new CamusException("CADB0000", "Response status code does not indicate success: 404 (Not Found).");

        await QueryAsync(connection, "SELECT 1");
        await QueryAsync(connection, "SELECT 2");

        Assert.Equal("unsupported",
            Assert.Single(Of("camusdb.client.prepared_statement.registrations"))["camusdb.prepared_statement.result"]);

        // Automatic preparation is off now, so the second statement is not a lookup at all.
        Assert.Single(Of("camusdb.client.prepared_statement.lookups"));
    }

    [Fact]
    public async Task Extensions_RegisterTheSourceAndTheMeterWithTheSdk()
    {
        List<Activity> exported = [];
        using TracerProvider tracer = Sdk.CreateTracerProviderBuilder()
            .AddCamusDBInstrumentation()
            .AddProcessor(new CaptureProcessor(exported, Database))
            .Build()!;

        MetricCapture capture = new();
        using MeterProvider meter = Sdk.CreateMeterProviderBuilder()
            .AddCamusDBInstrumentation()
            .AddReader(new BaseExportingMetricReader(capture))
            .Build()!;

        (CamusConnection connection, _, _) = Connect();
        await QueryAsync(connection, "SELECT 1");

        Activity span = Assert.Single(exported);
        Assert.Equal($"SELECT {Database}", span.DisplayName);

        Assert.True(meter.ForceFlush());
        Assert.Contains(capture.Points, point => point.Metric == "db.client.operation.duration"
            && point.Tags.GetValueOrDefault("db.namespace") as string == Database);
    }

    [Fact]
    public void Extensions_RejectANullBuilder()
    {
        Assert.Throws<ArgumentNullException>(() => ((TracerProviderBuilder)null!).AddCamusDBInstrumentation());
        Assert.Throws<ArgumentNullException>(() => ((MeterProviderBuilder)null!).AddCamusDBInstrumentation());
    }

    private sealed class FakeLoginClient : ICamusLoginClient
    {
        private int logins;

        public Task<CamusLoginResult> LoginAsync(string endpoint, string user, string password, int timeoutSeconds, CancellationToken cancellationToken)
            => Task.FromResult(new CamusLoginResult("token-" + Interlocked.Increment(ref logins), null));

        public Task LogoutAsync(string endpoint, string token, int timeoutSeconds, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class CaptureProcessor(List<Activity> into, string database) : BaseProcessor<Activity>
    {
        public override void OnEnd(Activity data)
        {
            if (data.GetTagItem("db.namespace") as string == database)
            {
                lock (into)
                    into.Add(data);
            }
        }
    }

    /// <summary>Copies each exported point, because the SDK reuses the metric objects after the export.</summary>
    private sealed class MetricCapture : BaseExporter<Metric>
    {
        public ConcurrentBag<(string Metric, Dictionary<string, object?> Tags)> Points { get; } = [];

        public override ExportResult Export(in Batch<Metric> batch)
        {
            foreach (Metric metric in batch)
            {
                foreach (ref readonly MetricPoint point in metric.GetMetricPoints())
                {
                    Dictionary<string, object?> tags = new();
                    foreach (KeyValuePair<string, object?> tag in point.Tags)
                        tags[tag.Key] = tag.Value;
                    Points.Add((metric.Name, tags));
                }
            }

            return ExportResult.Success;
        }
    }
}
