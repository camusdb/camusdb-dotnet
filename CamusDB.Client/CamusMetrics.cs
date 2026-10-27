/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace CamusDB.Client;

/// <summary>
/// The driver's own metrics, beside the <c>db.client.operation.duration</c> histogram of
/// <see cref="CamusTelemetry"/>: logins, retries, endpoint quarantine and the prepared-statement cache. All of
/// them are on the <see cref="CamusTelemetry.MeterName"/> meter. Each record method checks that a listener
/// enabled its instrument before it builds tags, so an unobserved process pays one check per event.
/// </summary>
internal static class CamusMetrics
{
    /// <summary>The <c>camusdb.retry.kind</c> values: what the driver retried.</summary>
    internal static class RetryKind
    {
        /// <summary>A <c>COMMIT</c> or <c>ROLLBACK</c> sent again after <c>CADB0509</c>.</summary>
        public const string Finalize = "finalize";

        /// <summary>An operation of <see cref="SerializableRetryHelper.ExecuteAutocommitAsync(Func{CancellationToken, Task}, int, CancellationToken)"/>
        /// run again after a serializable conflict.</summary>
        public const string Autocommit = "autocommit";

        /// <summary>A call sent again with a new token after the server refused the old one.</summary>
        public const string TokenRenewal = "token_renewal";

        /// <summary>A <c>CREATE DATABASE</c> or branch creation sent again after a transient failure.</summary>
        public const string CreateDatabase = "create_database";
    }

    /// <summary>The <c>camusdb.prepared_statement.result</c> values of a lookup.</summary>
    internal static class LookupResult
    {
        /// <summary>The statement was already prepared and went as a handle.</summary>
        public const string Hit = "hit";

        /// <summary>The statement was prepared by this execution and went as a handle.</summary>
        public const string Registered = "registered";

        /// <summary>The statement went inline, as SQL text.</summary>
        public const string Miss = "miss";
    }

    /// <summary>The <c>camusdb.prepared_statement.result</c> values of a registration.</summary>
    internal static class RegistrationResult
    {
        public const string Success = "success";

        /// <summary>The server refused this statement. It runs inline from now on.</summary>
        public const string Refused = "refused";

        /// <summary>The server has no prepared-statement support. The driver stops automatic preparation.</summary>
        public const string Unsupported = "unsupported";
    }

    /// <summary>The <c>camusdb.login.mode</c> values: how a login was started.</summary>
    internal static class LoginMode
    {
        /// <summary>A statement found no token it could send and waited for the login.</summary>
        public const string Blocking = "blocking";

        /// <summary>A token reached its renewal point. Statements kept it, and the login ran beside them.</summary>
        public const string Background = "background";

        /// <summary>The application called the login itself.</summary>
        public const string Explicit = "explicit";
    }

    internal static readonly Counter<long> Logins = CamusTelemetry.Meter.CreateCounter<long>(
        "camusdb.client.auth.logins",
        unit: "{login}",
        description: "Logins the driver sent to obtain a bearer token, by whether a statement waited for them.");

    internal static readonly Counter<long> Retries = CamusTelemetry.Meter.CreateCounter<long>(
        "camusdb.client.operation.retries",
        unit: "{retry}",
        description: "Operations the driver sent again after a failure it knows to be safe to retry.");

    internal static readonly Counter<long> EndpointQuarantines = CamusTelemetry.Meter.CreateCounter<long>(
        "camusdb.client.endpoint.quarantines",
        unit: "{quarantine}",
        description: "Times an endpoint failed at the transport level and was set aside for 30 seconds.");

    internal static readonly ObservableGauge<int> EndpointQuarantined = CamusTelemetry.Meter.CreateObservableGauge(
        "camusdb.client.endpoint.quarantined",
        ObserveEndpoints,
        unit: "{endpoint}",
        description: "1 while the endpoint is set aside after a transport failure, 0 while it takes traffic.");

    internal static readonly Counter<long> PreparedLookups = CamusTelemetry.Meter.CreateCounter<long>(
        "camusdb.client.prepared_statement.lookups",
        unit: "{lookup}",
        description: "Executions of a preparable statement, by whether the statement went as a prepared handle.");

    internal static readonly Counter<long> PreparedRegistrations = CamusTelemetry.Meter.CreateCounter<long>(
        "camusdb.client.prepared_statement.registrations",
        unit: "{registration}",
        description: "Requests to the server to prepare a statement.");

    internal static readonly Counter<long> PreparedEvictions = CamusTelemetry.Meter.CreateCounter<long>(
        "camusdb.client.prepared_statement.evictions",
        unit: "{eviction}",
        description: "Prepared statements released to make room for a statement used more recently.");

    internal static readonly ObservableUpDownCounter<int> PreparedCount = CamusTelemetry.Meter.CreateObservableUpDownCounter(
        "camusdb.client.prepared_statement.count",
        ObservePreparedCounts,
        unit: "{statement}",
        description: "Statements kept prepared now.");

    /// <summary>Creates the instruments. The observable ones report only after they exist, so
    /// <see cref="CamusTelemetry"/> calls this when it creates the meter. It reads a field, because a call
    /// alone does not make the runtime run the field initializers.</summary>
    internal static bool Initialize() => PreparedCount is not null;

    /// <summary>Counts one retry, and adds an event to the driver span that is current, if there is one.</summary>
    internal static void RecordRetry(string kind, string? errorType, string? database = null, string? operationName = null)
    {
        if (Activity.Current is { } current && ReferenceEquals(current.Source, CamusTelemetry.Source))
            current.AddEvent(new ActivityEvent("camusdb.retry", tags: new ActivityTagsCollection
            {
                { "camusdb.retry.kind", kind },
                { "error.type", errorType },
            }));

        if (!Retries.Enabled)
            return;

        TagList tags = new() { { "camusdb.retry.kind", kind } };
        if (errorType is not null)
            tags.Add("error.type", errorType);
        if (database is not null)
            tags.Add("db.namespace", database);
        if (operationName is not null)
            tags.Add("db.operation.name", operationName);

        Retries.Add(1, tags);
    }

    /// <summary>Counts one login. A blocking login also adds an event to the driver span that is current, if
    /// there is one, because that span carries the time of the login.</summary>
    internal static void RecordLogin(string mode, string? errorType)
    {
        if (mode == LoginMode.Blocking && Activity.Current is { } current && ReferenceEquals(current.Source, CamusTelemetry.Source))
            current.AddEvent(new ActivityEvent("camusdb.login", tags: new ActivityTagsCollection
            {
                { "camusdb.login.mode", mode },
                { "error.type", errorType },
            }));

        if (!Logins.Enabled)
            return;

        TagList tags = new() { { "camusdb.login.mode", mode } };
        if (errorType is not null)
            tags.Add("error.type", errorType);

        Logins.Add(1, tags);
    }

    internal static void RecordQuarantine(string endpoint)
    {
        if (!EndpointQuarantines.Enabled)
            return;

        EndpointQuarantines.Add(1, EndpointTags(endpoint));
    }

    internal static void RecordLookup(string database, string result)
    {
        if (!PreparedLookups.Enabled)
            return;

        PreparedLookups.Add(1, new TagList
        {
            { "db.namespace", database },
            { "camusdb.prepared_statement.result", result },
        });
    }

    internal static void RecordRegistration(string database, string result, string? errorType = null)
    {
        if (!PreparedRegistrations.Enabled)
            return;

        TagList tags = new()
        {
            { "db.namespace", database },
            { "camusdb.prepared_statement.result", result },
        };
        if (errorType is not null)
            tags.Add("error.type", errorType);

        PreparedRegistrations.Add(1, tags);
    }

    internal static void RecordEviction(string database)
    {
        if (!PreparedEvictions.Enabled)
            return;

        PreparedEvictions.Add(1, new TagList { { "db.namespace", database } });
    }

    private static TagList EndpointTags(string endpoint)
    {
        (string host, int port) = CamusOperation.ParseEndpoint(endpoint);

        TagList tags = new() { { "server.address", host } };
        if (port > 0)
            tags.Add("server.port", port);
        return tags;
    }

    /// <summary>One measurement per endpoint of the shared pools. Two pools can list the same endpoint;
    /// it counts as set aside when one of them set it aside.</summary>
    private static IEnumerable<Measurement<int>> ObserveEndpoints()
    {
        Dictionary<string, bool> quarantined = new(StringComparer.OrdinalIgnoreCase);
        CamusEndpointPool.CollectQuarantine(quarantined);

        List<Measurement<int>> measurements = new(quarantined.Count);
        foreach ((string endpoint, bool isQuarantined) in quarantined)
            measurements.Add(new Measurement<int>(isQuarantined ? 1 : 0, EndpointTags(endpoint)));
        return measurements;
    }

    /// <summary>One measurement per database of the shared policies.</summary>
    private static IEnumerable<Measurement<int>> ObservePreparedCounts()
    {
        Dictionary<string, int> counts = new(StringComparer.Ordinal);
        CamusPreparedStatementPolicy.CollectPreparedCounts(counts);

        List<Measurement<int>> measurements = new(counts.Count);
        foreach ((string database, int count) in counts)
            measurements.Add(new Measurement<int>(count, new TagList { { "db.namespace", database } }));
        return measurements;
    }
}
