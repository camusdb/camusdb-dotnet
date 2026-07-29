
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client.Auth;
using CamusDB.Client.Transport;

namespace CamusDB.Client;

/// <summary>
/// Represents a connection builder class
/// </summary>
public class CamusConnectionStringBuilder
{
    private readonly string connectionString;

    public SessionPoolManager? SessionPoolManager { get; set; }

    public Dictionary<string, string> Config { get; } = new();

    private CamusEndpointPool? endpointPool;

    private ICamusTransport? transport;

    private CamusTokenProvider? tokenProvider;

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
                ? new GrpcTransport(auth)
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
