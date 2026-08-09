
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client.Auth;
using CamusDB.Client.Transport;
using CamusDB.Client.Transport.Batching;

namespace CamusDB.Client;

/// <summary>
/// Represents a connection builder class
/// </summary>
public class CamusConnectionStringBuilder
{
    private readonly string connectionString;

    [Obsolete("CamusDB has no server-side sessions to pool; this property is never consulted. Tune the gRPC stream pool with the ChannelPoolSize= connection-string key instead.")]
    public SessionPoolManager? SessionPoolManager { get; set; }

    public Dictionary<string, string> Config { get; } = new();

    private CamusEndpointPool? endpointPool;

    private ICamusTransport? transport;

    private CamusTokenProvider? tokenProvider;

    private CamusPreparedStatementPolicy? preparedStatements;

    private readonly object transportLock = new();

    private readonly object authLock = new();

    public CamusConnectionStringBuilder(string connectionString)
    {
        this.connectionString = connectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        string[] settings = connectionString.Split(";");        

        foreach (string setting in settings)
        {
            string[] varParts = setting.Split("=", 2);
            if (varParts.Length != 2)
                continue;

            Config.TryAdd(varParts[0], varParts[1]);
        }
    }

    /// <summary>
    /// Command timeout in seconds. Read from the "Timeout" key in the connection string.
    /// Defaults to 10 seconds.
    /// </summary>
    public int CommandTimeout
    {
        get
        {
            if (Config.TryGetValue("Timeout", out string? raw) && int.TryParse(raw, out int seconds) && seconds > 0)
                return seconds;
            return 10;
        }
    }

    /// <summary>
    /// Connection-wide default concurrency options parsed from the <c>IsolationLevel=</c>,
    /// <c>TransactionMode=</c> and <c>Locking=</c> connection-string keys (case-insensitive values). Any
    /// key that is absent or unrecognized leaves the corresponding knob <see langword="null"/> (server
    /// default). A per-transaction <see cref="CamusTransactionOptions"/> overrides these.
    /// </summary>
    public CamusTransactionOptions DefaultTransactionOptions => new()
    {
        IsolationLevel = ParseEnum<CamusIsolationLevel>("IsolationLevel"),
        Mode = ParseEnum<CamusTransactionMode>("TransactionMode"),
        Locking = ParseEnum<CamusLocking>("Locking"),
    };

    /// <summary>
    /// Batcher tuning for gRPC connections, from <c>ChannelPoolSize=</c>, <c>CoalescingThreshold=</c> and
    /// <c>CoalescingDelay=</c> (milliseconds). Ignored by the REST transport.
    ///
    /// <para><c>ChannelPoolSize</c> is the CamusDB analogue of Spanner's <c>NumChannels</c>: how many
    /// long-lived <c>BatchExecute</c> streams exist per endpoint. It is <i>not</i> a cap on in-flight
    /// transactions — many transactions hash onto the same streams and interleave — so the default of 2 is
    /// right for most workloads. Raise it when many long-running streaming queries per endpoint would
    /// otherwise queue behind each other on a shared stream.</para>
    ///
    /// <para>Out-of-range values (a pool below 1, a threshold below 1, a negative delay) fall back to the
    /// default rather than throwing, matching how the other keys treat unparseable input.</para>
    /// </summary>
    internal GrpcBatchOptions BatchOptions => new()
    {
        ChannelPoolSize = ParseInt("ChannelPoolSize", 1, GrpcBatchOptions.Default.ChannelPoolSize),
        CoalescingThreshold = ParseInt("CoalescingThreshold", 1, GrpcBatchOptions.Default.CoalescingThreshold),
        CoalescingDelayMs = ParseInt("CoalescingDelay", 0, GrpcBatchOptions.Default.CoalescingDelayMs),
    };

    private int ParseInt(string key, int minimum, int fallback)
        => Config.TryGetValue(key, out string? raw) && int.TryParse(raw, out int value) && value >= minimum
            ? value
            : fallback;

    private T? ParseEnum<T>(string key) where T : struct, Enum
        => Config.TryGetValue(key, out string? raw) && Enum.TryParse(raw, ignoreCase: true, out T value)
            ? value
            : null;

    /// <summary>
    /// The wire protocol this connection speaks, from the <c>Protocol=</c> connection-string key
    /// (case-insensitive: <c>rest</c> or <c>grpc</c>). Absent or unrecognized values default to
    /// <see cref="CamusProtocol.Rest"/>. When <see cref="CamusProtocol.Grpc"/> is selected, the
    /// <c>Endpoint=</c> must address the server's gRPC port.
    /// </summary>
    public CamusProtocol Protocol => ParseEnum<CamusProtocol>("Protocol") ?? CamusProtocol.Rest;

    /// <summary>
    /// How many distinct statements this connection string keeps prepared on the server, from
    /// <c>MaxAutoPrepare=</c>. <c>0</c> turns automatic preparation off, leaving only explicit
    /// <see cref="CamusCommand.Prepare"/>. Defaults to
    /// <see cref="CamusPreparedStatementPolicy.DefaultMaxAutoPrepare"/>.
    /// </summary>
    public int MaxAutoPrepare
        => Config.TryGetValue("MaxAutoPrepare", out string? raw) && int.TryParse(raw, out int max) && max >= 0
            ? max
            : CamusPreparedStatementPolicy.DefaultMaxAutoPrepare;

    /// <summary>
    /// How many times the same SQL must run before it is prepared, from <c>AutoPrepareMinUsages=</c>.
    /// Defaults to <see cref="CamusPreparedStatementPolicy.DefaultMinUsages"/>. Raise it for a workload
    /// that issues many near-unique statements; lower it to 1 to prepare on first sight.
    /// </summary>
    public int AutoPrepareMinUsages
        => Config.TryGetValue("AutoPrepareMinUsages", out string? raw) && int.TryParse(raw, out int usages) && usages > 0
            ? usages
            : CamusPreparedStatementPolicy.DefaultMinUsages;

    /// <summary>
    /// How many statements this connection string currently keeps prepared on the server. Zero before
    /// anything has been prepared — and while auto-preparation is off — so it also answers "is this
    /// connection preparing anything at all?".
    /// </summary>
    public int PreparedStatementCount => PreparedStatements.PreparedCount;

    /// <summary>
    /// Whether <paramref name="sql"/> is currently kept prepared for this connection string's database.
    /// Answers "did that statement get prepared?" without depending on what else the process has run,
    /// which <see cref="PreparedStatementCount"/> cannot, since the decision is shared per deployment.
    /// </summary>
    public bool IsPrepared(string sql)
        => PreparedStatements.IsPrepared(Config.TryGetValue("Database", out string? database) ? database : "", sql);

    /// <summary>
    /// Which statements this connection string's commands prepare.
    ///
    /// <para>Shared process-wide by deployment and settings rather than owned by this builder. Which
    /// statements a workload repeats is a property of the workload, not of one short-lived builder — and
    /// EF Core creates a builder per <c>DbContext</c>, so a builder-owned policy would restart its
    /// counting on every request and never conclude anything.</para>
    /// </summary>
    internal CamusPreparedStatementPolicy PreparedStatements
    {
        get
        {
            if (preparedStatements is not null)
                return preparedStatements;

            lock (transportLock)
            {
                int max = MaxAutoPrepare;
                int usages = AutoPrepareMinUsages;
                string database = Config.TryGetValue("Database", out string? name) ? name : "";

                return preparedStatements ??= CamusPreparedStatementPolicy.Shared(
                    $"{DeploymentKey}|{database}|{max}|{usages}",
                    () => new CamusPreparedStatementPolicy(max, usages));
            }
        }
    }

    /// <summary>
    /// Credentials read from the connection string. Either <c>User=</c> (aliases <c>UserId</c>,
    /// <c>Uid</c>, <c>Username</c>) plus <c>Password=</c> (alias <c>Pwd</c>), which the driver exchanges
    /// for a bearer token and re-exchanges as needed, or <c>AccessToken=</c> for a token obtained
    /// elsewhere. Neither key present means unauthenticated, which is what a default CamusDB install
    /// expects.
    /// </summary>
    internal CamusCredentials Credentials
    {
        get
        {
            if (TryGetSetting(out string? accessToken, "AccessToken"))
                return CamusCredentials.FromToken(accessToken);

            if (TryGetSetting(out string? user, "User", "UserId", "Uid", "Username"))
                return CamusCredentials.FromPassword(user, Config.TryGetValue("Password", out string? password)
                    ? password
                    : Config.TryGetValue("Pwd", out string? pwd) ? pwd : "");

            return CamusCredentials.None;
        }
    }

    /// <summary>
    /// How long the driver reuses a minted token before logging in again, from <c>TokenLifetime=</c>
    /// (seconds), defaulting to 10 minutes. Only consulted when the server does not report the token's
    /// expiry — when it does, that value wins, since its <c>AccessTokenTtl</c> is configurable and may be
    /// shorter than anything set here. Either way an expiry missed on the client is caught reactively and
    /// the statement replayed.
    /// </summary>
    internal TimeSpan TokenLifetime
        => Config.TryGetValue("TokenLifetime", out string? raw) && int.TryParse(raw, out int seconds) && seconds > 0
            ? TimeSpan.FromSeconds(seconds)
            : CamusTokenProvider.DefaultLifetime;

    /// <summary>
    /// The token provider shared by every connection built from this builder, so a pool of connections
    /// performs one login rather than one each.
    /// </summary>
    internal CamusTokenProvider TokenProvider
    {
        get
        {
            if (tokenProvider is not null)
                return tokenProvider;

            lock (authLock)
            {
                if (tokenProvider is not null)
                    return tokenProvider;

                CamusCredentials credentials = Credentials;

                CamusTokenProvider Create() => new(
                    credentials,
                    GetLoginClient,
                    GetEndpoint,
                    () => CommandTimeout,
                    TokenLifetime);

                // Configured credentials are shared process-wide so repeatedly-rebuilt connections (EF
                // opens one per operation) reuse a single token. With nothing configured the provider is
                // inert and stays private to this builder, so a later CamusConnection.LoginAsync only
                // affects the connections built from this connection string.
                return tokenProvider = credentials.IsSet
                    ? CamusTokenProvider.Shared(CamusTokenProvider.SharingKey(credentials, DeploymentKey), Create)
                    : Create();
            }
        }
    }

    /// <summary>
    /// The transport this builder's connections use, chosen once from <see cref="Protocol"/> and cached
    /// for the builder's lifetime (a gRPC transport pools long-lived channels, so it must be shared, not
    /// recreated per call). It is always wrapped in <see cref="AuthenticatingTransport"/>: with no
    /// credentials configured that wrapper is inert, and wrapping unconditionally means a connection
    /// authenticated later — via <see cref="CamusConnection.LoginAsync"/> — is covered too.
    /// </summary>
    internal ICamusTransport GetTransport()
    {
        if (transport is not null)
            return transport;

        lock (transportLock)
        {
            if (transport is not null)
                return transport;

            CamusTokenProvider auth = TokenProvider;

            ICamusTransport inner = Protocol == CamusProtocol.Grpc
                ? new GrpcTransport(auth, BatchOptions)
                : new RestTransport(this, auth);

            return transport = new AuthenticatingTransport(inner, auth);
        }
    }

    private bool TryGetSetting([System.Diagnostics.CodeAnalysis.NotNullWhen(true)] out string? value, params string[] keys)
    {
        foreach (string key in keys)
        {
            if (Config.TryGetValue(key, out value) && !string.IsNullOrWhiteSpace(value))
                return true;
        }

        value = null;
        return false;
    }

    internal string GetEndpoint()
    {
        if (!Config.TryGetValue("Endpoint", out string? endpoint) || string.IsNullOrWhiteSpace(endpoint))
            throw new CamusException("CADB0000", "Endpoint is required");

        endpointPool ??= new CamusEndpointPool(endpoint);

        return endpointPool.GetNextEndpoint();
    }

    /// <summary>
    /// Identifies the deployment when deciding which connections may share one token: the endpoint pool
    /// plus the protocol, so the same credentials against two different servers never share a token, and
    /// a REST and a gRPC connection each hold the token minted by their own transport.
    /// </summary>
    private string DeploymentKey
        => $"{(Config.TryGetValue("Endpoint", out string? endpoint) ? endpoint : "")}|{Protocol}";

    /// <summary>
    /// Who performs the credential exchange. gRPC connections use the <c>CamusAuth</c> service on the
    /// transport's own channel; REST connections post to <c>/login</c>. Either way the token is obtained
    /// over the same protocol and endpoint that carries the statements — there is no second port to
    /// configure and no cross-protocol hop.
    /// </summary>
    private ICamusLoginClient GetLoginClient()
        => GetTransport() is AuthenticatingTransport { Inner: ICamusLoginClient grpc } ? grpc : new RestLoginClient();

    internal void MarkEndpointUnreachable(string endpoint)
    {
        endpointPool?.MarkUnreachable(endpoint);
    }

    public override string ToString() => connectionString;
}
