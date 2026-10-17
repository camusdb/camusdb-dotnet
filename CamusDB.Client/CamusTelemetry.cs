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

namespace CamusDB.Client;

/// <summary>
/// The names of the OpenTelemetry sources this driver emits on. The driver uses the
/// <see cref="ActivitySource"/> and <see cref="Meter"/> types of the base library, so it needs no
/// OpenTelemetry package: register the names with the OpenTelemetry SDK to collect the data.
///
/// <code>
/// builder.Services.AddOpenTelemetry()
///     .WithTracing(t => t.AddSource(CamusTelemetry.ActivitySourceName))
///     .WithMetrics(m => m.AddMeter(CamusTelemetry.MeterName));
/// </code>
///
/// <para>The driver records one client span per command execution, per <c>BEGIN</c>, per
/// <c>COMMIT</c> or <c>ROLLBACK</c>, and per pipeline exchange, with the attributes of the OpenTelemetry
/// database semantic conventions. It records the duration of the same operations on the
/// <c>db.client.operation.duration</c> histogram, and its retries, endpoint quarantine and
/// prepared-statement cache on the <c>camusdb.client.*</c> instruments (see <see cref="CamusMetrics"/>).
/// When nothing listens, the cost is one check per operation.</para>
///
/// <para>The span does not contain the SQL text unless the connection string sets
/// <c>TraceQueryText=true</c>, because SQL text with literal values can contain sensitive data. The
/// driver never records parameter values.</para>
/// </summary>
public static class CamusTelemetry
{
    /// <summary>The name of the <see cref="ActivitySource"/> that emits the driver's spans.</summary>
    public const string ActivitySourceName = "CamusDB.Client";

    /// <summary>The name of the <see cref="Meter"/> that emits the driver's metrics.</summary>
    public const string MeterName = "CamusDB.Client";

    private static readonly string? Version = typeof(CamusTelemetry).Assembly.GetName().Version?.ToString();

    internal static readonly ActivitySource Source = new(ActivitySourceName, Version);

    internal static readonly Meter Meter = new(MeterName, Version);

    /// <summary>The semantic-convention histogram, in seconds, with the bucket boundaries it recommends.</summary>
    internal static readonly Histogram<double> OperationDuration = Meter.CreateHistogram<double>(
        "db.client.operation.duration",
        unit: "s",
#if NET9_0_OR_GREATER
        description: "Duration of database client operations.",
        tags: null,
        advice: new InstrumentAdvice<double> { HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10] });
#else
        description: "Duration of database client operations.");
#endif

    // After Meter: the instruments of CamusMetrics are created on it.
    private static readonly bool MetricsCreated = CamusMetrics.Initialize();
}

/// <summary>
/// One instrumented operation: its span, if a listener sampled it, and its start time, if the duration
/// histogram is enabled. <see cref="Start"/> returns <see langword="null"/> when neither is the case, so
/// an uninstrumented process allocates nothing. The operation ends once: the first
/// <see cref="Complete"/> or <see cref="Fail(Exception)"/> ends it, and later calls do nothing.
/// </summary>
internal sealed class CamusOperation
{
    internal const string SystemName = "camusdb";

    /// <summary>The operation names the driver reads from the first keyword of the SQL text. A keyword
    /// that is not in this list gives no operation name, so the metric keeps a small set of values.</summary>
    private static readonly string[] KnownOperations =
    [
        "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE",
        "EXPLAIN", "SHOW", "REFRESH", "COMMENT", "GRANT", "REVOKE", "SET",
    ];

    /// <summary>The parsed host and port of each endpoint. Endpoints come from the connection string and
    /// the cluster, so the set stays small.</summary>
    private static readonly ConcurrentDictionary<string, (string Host, int Port)> Endpoints = new(StringComparer.Ordinal);

    private readonly Activity? activity;

    private readonly long startTimestamp;

    private readonly string? database;

    private readonly string? operationName;

    private string? serverAddress;

    private int serverPort;

    private int ended;

    private CamusOperation(Activity? activity, long startTimestamp, string? database, string? operationName)
    {
        this.activity = activity;
        this.startTimestamp = startTimestamp;
        this.database = database;
        this.operationName = operationName;
    }

    /// <summary>Whether a listener or the duration histogram wants operations. A caller checks this before
    /// it computes the arguments of <see cref="Start"/>.</summary>
    internal static bool IsEnabled => CamusTelemetry.OperationDuration.Enabled || CamusTelemetry.Source.HasListeners();

    /// <summary>The span of this operation, or <see langword="null"/> when no listener sampled it.</summary>
    internal Activity? Activity => activity;

    /// <summary>
    /// Starts an operation. <paramref name="queryText"/> goes on the span only when the connection string
    /// sets <c>TraceQueryText=true</c>. <paramref name="collection"/> is the table name, when the driver
    /// knows it without a parse of the SQL text.
    /// </summary>
    internal static CamusOperation? Start(
        CamusConnectionStringBuilder builder, string? operationName, string? collection = null, string? queryText = null, int batchSize = 0)
    {
        bool timed = CamusTelemetry.OperationDuration.Enabled;
        if (!timed && !CamusTelemetry.Source.HasListeners())
            return null;

        builder.Config.TryGetValue("Database", out string? database);

        Activity? activity = CamusTelemetry.Source.StartActivity(SpanName(operationName, collection, database), ActivityKind.Client);
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag("db.system.name", SystemName);
            if (database is not null)
                activity.SetTag("db.namespace", database);
            if (operationName is not null)
                activity.SetTag("db.operation.name", operationName);
            if (collection is not null)
                activity.SetTag("db.collection.name", collection);
            if (batchSize > 0)
                activity.SetTag("db.operation.batch.size", batchSize);
            if (queryText is not null && builder.TraceQueryText)
                activity.SetTag("db.query.text", queryText);
            activity.SetTag("camusdb.transport", builder.Protocol == CamusProtocol.Grpc ? "grpc" : "rest");
        }

        if (activity is null && !timed)
            return null;

        return new CamusOperation(activity, timed ? Stopwatch.GetTimestamp() : 0, database, operationName);
    }

    /// <summary>The span name the conventions give: <c>{operation} {target}</c>, where the target is the
    /// table when known and the database otherwise.</summary>
    private static string SpanName(string? operationName, string? collection, string? database)
    {
        string? target = collection ?? database;

        if (operationName is null)
            return target ?? SystemName;

        return target is null ? operationName : string.Concat(operationName, " ", target);
    }

    /// <summary>
    /// The operation name for <paramref name="sql"/>: its first keyword in upper case when that keyword is
    /// a known statement verb, and <see langword="null"/> otherwise. Leading spaces and opening parentheses
    /// are skipped.
    /// </summary>
    internal static string? OperationName(string? sql)
    {
        if (sql is null)
            return null;

        ReadOnlySpan<char> text = sql.AsSpan().TrimStart();
        while (!text.IsEmpty && text[0] == '(')
            text = text[1..].TrimStart();

        int length = 0;
        while (length < text.Length && char.IsAsciiLetter(text[length]))
            length++;

        ReadOnlySpan<char> keyword = text[..length];
        foreach (string known in KnownOperations)
        {
            if (keyword.Equals(known, StringComparison.OrdinalIgnoreCase))
                return known;
        }

        return null;
    }

    /// <summary>The <c>server.address</c> and <c>server.port</c> of <paramref name="endpoint"/>. The port is
    /// 0 when the endpoint is not an absolute URI.</summary>
    internal static (string Host, int Port) ParseEndpoint(string endpoint)
        => Endpoints.GetOrAdd(endpoint, static e =>
            Uri.TryCreate(e, UriKind.Absolute, out Uri? uri) ? (uri.IdnHost, uri.Port) : (e, 0));

    /// <summary>Records the server the operation goes to, as <c>server.address</c> and <c>server.port</c>.</summary>
    internal void SetEndpoint(string endpoint)
    {
        (serverAddress, serverPort) = ParseEndpoint(endpoint);

        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag("server.address", serverAddress);
            if (serverPort > 0)
                activity.SetTag("server.port", serverPort);
        }
    }

    /// <summary>Ends the operation as a success.</summary>
    internal void Complete() => End(null);

    /// <summary>Ends the operation as a failure.</summary>
    internal void Fail(Exception exception) => End(exception);

    /// <summary>
    /// Ends <paramref name="operation"/> as a failure and returns <see langword="false"/>, for use as an
    /// exception filter: the exception continues up the stack with its original trace.
    /// </summary>
    internal static bool Fail(CamusOperation? operation, Exception exception)
    {
        operation?.End(exception);
        return false;
    }

    private void End(Exception? exception)
    {
        if (Interlocked.Exchange(ref ended, 1) != 0)
            return;

        // The conventions use the database status code as the error type when there is one.
        string? statusCode = (exception as CamusException)?.Code;
        string? errorType = exception is null ? null : statusCode ?? exception.GetType().FullName;

        if (activity is not null)
        {
            if (exception is not null)
            {
                if (activity.IsAllDataRequested)
                {
                    activity.SetTag("error.type", errorType);
                    if (statusCode is not null)
                        activity.SetTag("db.response.status_code", statusCode);
#if NET9_0_OR_GREATER
                    activity.AddException(exception);
#endif
                }
                activity.SetStatus(ActivityStatusCode.Error, exception.Message);
            }

            // Stop() makes the span's parent the current activity. A reader can end its span in a later
            // call of the application, where another activity is current, so keep that one current.
            Activity? current = Activity.Current;
            activity.Stop();
            if (!ReferenceEquals(current, activity))
                Activity.Current = current;
        }

        if (startTimestamp != 0)
        {
            TagList tags = new() { { "db.system.name", SystemName } };
            if (database is not null)
                tags.Add("db.namespace", database);
            if (operationName is not null)
                tags.Add("db.operation.name", operationName);
            if (serverAddress is not null)
            {
                tags.Add("server.address", serverAddress);
                if (serverPort > 0)
                    tags.Add("server.port", serverPort);
            }
            if (errorType is not null)
                tags.Add("error.type", errorType);
            if (statusCode is not null)
                tags.Add("db.response.status_code", statusCode);

            CamusTelemetry.OperationDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, tags);
        }
    }
}
