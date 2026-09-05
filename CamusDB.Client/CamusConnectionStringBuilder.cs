
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

    /// <summary>
    /// The parsed keys. Keys are matched without regard to case, so <c>password=</c> reaches the same
    /// entry as <c>Password=</c> — a lowercase key used to be dropped silently, which left the driver
    /// connecting with no credentials at all against a server that happened to allow it.
    /// </summary>
    public Dictionary<string, string> Config { get; } = new(StringComparer.OrdinalIgnoreCase);

    private CamusEndpointPool? endpointPool;

    private volatile ICamusTransport? transport;

    private volatile CamusTokenProvider? tokenProvider;

    private CamusPreparedStatementPolicy? preparedStatements;

    /// <summary>Set when this connection string must keep its own identity — see
    /// <see cref="DetachForExplicitLogin"/>. Read on the lock-free fast paths, so volatile.</summary>
    private volatile bool privateAuth;

    private readonly object transportLock = new();

    private readonly object authLock = new();

    /// <summary>
    /// Parses a <c>key=value;key=value</c> connection string.
    ///
    /// <para>Keys are matched without regard to case and are trimmed, so <c>password=</c> and
    /// <c> Password =</c> both reach the <c>Password</c> entry. A repeated key is an error rather than a
    /// silent first-wins: the two spellings usually disagree, and the one that lost used to disappear
    /// without a diagnostic.</para>
    ///
    /// <para>An unquoted value is trimmed and ends at the next <c>;</c>. A value that must carry a
    /// semicolon, or leading or trailing spaces, is written in single or double quotes
    /// (<c>Password='p;w'</c>), doubling the quote character to include it (<c>Password='p''w'</c>).
    /// Without quoting such a password had no spelling at all — it was silently truncated at the
    /// semicolon.</para>
    ///
    /// <para>A segment with no <c>=</c> is ignored, which is what an empty or whitespace-only connection
    /// string relies on.</para>
    /// </summary>
    /// <exception cref="CamusException">CADB0000 when a key appears twice, or a quoted value is unclosed.</exception>
    public CamusConnectionStringBuilder(string connectionString)
    {
        this.connectionString = connectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        foreach (Setting setting in EnumerateSettings(connectionString))
        {
            string key = connectionString.Substring(setting.KeyStart, setting.KeyLength);

            if (!Config.TryAdd(key, setting.Value(connectionString)))
                throw new CamusException(
                    "CADB0000",
                    $"The connection string sets '{key}' more than once. Keys are matched without regard to case; remove the duplicate.");
        }

        EnsureCredentialsAreNotSentInClear();
    }

    /// <summary>One parsed key/value pair, as positions in the connection string. Spans rather than
    /// substrings so parsing a builder — which Entity Framework does once per connection — allocates only
    /// the strings that are kept.</summary>
    private readonly struct Setting(int keyStart, int keyLength, int valueStart, int valueLength, bool quoted, char quote)
    {
        public int KeyStart { get; } = keyStart;

        public int KeyLength { get; } = keyLength;

        public int ValueStart { get; } = valueStart;

        public int ValueLength { get; } = valueLength;

        /// <summary>True when the value was written in quotes, so its text still carries doubled quotes.</summary>
        public bool Quoted { get; } = quoted;

        public char Quote { get; } = quote;

        public string Value(string source)
        {
            string raw = source.Substring(ValueStart, ValueLength);

            return Quoted ? raw.Replace(new string(Quote, 2), Quote.ToString(), StringComparison.Ordinal) : raw;
        }
    }

    private static List<Setting> EnumerateSettings(string source)
    {
        List<Setting> settings = [];
        int position = 0;

        while (position < source.Length)
        {
            // Key: up to the next '=' or ';'.
            int keyStart = position;
            while (position < source.Length && source[position] != '=' && source[position] != ';')
                position++;

            if (position >= source.Length || source[position] == ';')
            {
                // A segment with no '=' carries nothing to set. Skip it and continue with the next.
                position++;
                continue;
            }

            int keyEnd = position;       // at '='
            position++;                  // past '='

            (int keyTrimmedStart, int keyTrimmedLength) = Trim(source, keyStart, keyEnd);

            // Value: quoted, or up to the next ';'.
            while (position < source.Length && source[position] is ' ' or '\t')
                position++;

            int valueStart;
            int valueLength;
            bool quoted = false;
            char quote = '\0';

            if (position < source.Length && source[position] is '\'' or '"')
            {
                quote = source[position];
                quoted = true;
                position++;
                valueStart = position;

                while (true)
                {
                    if (position >= source.Length)
                        throw new CamusException(
                            "CADB0000",
                            $"The connection string has an unclosed {quote} quoted value. Double the quote character to include one in a value.");

                    if (source[position] == quote)
                    {
                        // A doubled quote is one literal quote, not the end of the value.
                        if (position + 1 < source.Length && source[position + 1] == quote)
                        {
                            position += 2;
                            continue;
                        }

                        break;
                    }

                    position++;
                }

                valueLength = position - valueStart;
                position++;              // past the closing quote

                // Anything between the closing quote and the ';' is whitespace or a typo; skip to the ';'.
                while (position < source.Length && source[position] != ';')
                    position++;
            }
            else
            {
                valueStart = position;

                while (position < source.Length && source[position] != ';')
                    position++;

                (valueStart, valueLength) = Trim(source, valueStart, position);
            }

            position++;                  // past the ';'

            if (keyTrimmedLength > 0)
                settings.Add(new Setting(keyTrimmedStart, keyTrimmedLength, valueStart, valueLength, quoted, quote));
        }

        return settings;
    }

    private static (int Start, int Length) Trim(string source, int start, int end)
    {
        while (start < end && char.IsWhiteSpace(source[start]))
            start++;

        while (end > start && char.IsWhiteSpace(source[end - 1]))
            end--;

        return (start, end - start);
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
    /// Timeout in seconds for the backup admin API, from <c>BackupTimeout=</c>. Defaults to 300 (5
    /// minutes) rather than <see cref="CommandTimeout"/>: taking a full or coordinated backup copies a
    /// whole node's base image, which routinely outlasts a statement timeout of ten seconds.
    /// </summary>
    public int BackupTimeout
        => Config.TryGetValue("BackupTimeout", out string? raw) && int.TryParse(raw, out int seconds) && seconds > 0
            ? seconds
            : 300;

    /// <summary>
    /// Where the backup admin API lives, from <c>BackupEndpoint=</c>, falling back to <c>Endpoint=</c>.
    ///
    /// <para>The backup endpoints are REST/JSON only — they have no SQL form and no gRPC service — so a
    /// <c>Protocol=grpc</c> connection, whose <c>Endpoint=</c> addresses the gRPC port, must say where the
    /// HTTP port is. Rather than send HTTP at a gRPC port and fail obscurely, that case is refused with a
    /// message naming the key to set.</para>
    ///
    /// <para>A <c>BackupEndpoint=</c> is used verbatim, with no round-robin: backups are node-level, and a
    /// coordinated backup must reach the coordinator specifically. Falling back to <c>Endpoint=</c> draws
    /// from the usual pool.</para>
    ///
    /// <para>It must name the same deployment as <c>Endpoint=</c>. The backup admin API is authorized with
    /// this connection's own token — superuser-grade, since that is what the surface requires — and a token
    /// is only meaningful to the deployment that minted it, so pointing this key elsewhere hands a
    /// superuser credential to a host that cannot use it and should never see it. It is held to the same
    /// transport rule as <c>Endpoint=</c>; see <see cref="EnsureCredentialsAreNotSentInClear"/>.</para>
    /// </summary>
    public string GetBackupEndpoint()
    {
        if (TryGetSetting(out string? backupEndpoint, "BackupEndpoint"))
            return backupEndpoint;

        if (Protocol != CamusProtocol.Rest)
            throw new CamusException(
                "CADB0000",
                "The backup admin API is REST-only; a Protocol=grpc connection must set BackupEndpoint= to the server's HTTP endpoint");

        return GetEndpoint();
    }

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

                EnsureCredentialsAreNotSentInClear();

                CamusCredentials credentials = Credentials;

                CamusTokenProvider Create() => new(
                    credentials,
                    GetLoginClient,
                    GetEndpoint,
                    () => CommandTimeout,
                    TokenLifetime);

                // Shared process-wide by identity so repeatedly-rebuilt connections (EF opens one per
                // operation) reuse a single token — and, with nothing configured, a single inert provider,
                // so the transport keyed beside it can be shared too. A connection string that logs in
                // explicitly having configured no credentials detaches first, which is what keeps that
                // login from reaching connections built from another connection string.
                return tokenProvider = privateAuth
                    ? Create()
                    : CamusTokenProvider.Shared(CamusTokenProvider.SharingKey(credentials, DeploymentKey), Create);
            }
        }
    }

    /// <summary>
    /// The transport this builder's connections use, chosen once from <see cref="Protocol"/>.
    ///
    /// <para>Shared process-wide by deployment, identity and transport tuning rather than owned by this
    /// builder — see <see cref="CamusTransportPool"/> for why: a transport holds the prepared-statement
    /// registrations, and EF Core builds a builder per <c>DbConnection</c>, so a builder-owned transport
    /// re-registered every hot statement on every request and never closed the ones it replaced.</para>
    ///
    /// <para>It is always wrapped in <see cref="AuthenticatingTransport"/>: with no credentials configured
    /// that wrapper is inert, and wrapping unconditionally means a connection authenticated later — via
    /// <see cref="CamusConnection.LoginAsync"/>, which detaches this builder onto a transport of its own
    /// first — is covered too.</para>
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

            ICamusTransport Create()
            {
                ICamusTransport inner = Protocol == CamusProtocol.Grpc
                    ? new GrpcTransport(auth, BatchOptions)
                    : new RestTransport(EndpointPool, auth);

                return new AuthenticatingTransport(inner, auth);
            }

            return transport = privateAuth ? Create() : CamusTransportPool.Shared(TransportKey, Create);
        }
    }

    /// <summary>
    /// Which transport this connection string may share. The deployment and the identity presented to it,
    /// so a transport is never reused across servers, protocols or users — the credentials are hashed by
    /// <see cref="CamusTokenProvider.SharingKey"/> rather than held in a long-lived dictionary key — plus
    /// the batch tuning, which sizes a gRPC transport's stream pool and so cannot be retrofitted onto one
    /// that already exists.
    /// </summary>
    private string TransportKey
    {
        get
        {
            GrpcBatchOptions batch = BatchOptions;

            return string.Join(
                '|',
                CamusTokenProvider.SharingKey(Credentials, DeploymentKey),
                batch.ChannelPoolSize,
                batch.CoalescingThreshold,
                batch.CoalescingDelayMs);
        }
    }

    /// <summary>
    /// Gives this connection string a token provider and transport of its own, and returns the provider to
    /// log in with.
    ///
    /// <para>Connections that configured no credentials share one inert provider per deployment, so that
    /// the transport keyed beside it can be shared too. An explicit
    /// <see cref="CamusConnection.LoginAsync"/> would otherwise authenticate every other connection string
    /// pointed at the same server that also configured nothing — connections whose caller deliberately
    /// presented no identity. Detaching first keeps the login where the caller aimed it: at the
    /// connections built from this connection string, which is exactly what it did when every builder had
    /// a provider to itself.</para>
    ///
    /// <para>Configured credentials are unaffected. Those already share a provider by identity, and a
    /// login that switches identity has always applied to all of them.</para>
    /// </summary>
    internal CamusTokenProvider DetachForExplicitLogin()
    {
        if (Credentials.IsSet)
            return TokenProvider;

        // Same order as GetTransport takes them, which reaches authLock through the TokenProvider getter.
        lock (transportLock)
        {
            lock (authLock)
            {
                if (!privateAuth)
                {
                    privateAuth = true;
                    tokenProvider = null;
                    transport = null;
                }
            }
        }

        return TokenProvider;
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

    internal string GetEndpoint() => EndpointPool.GetNextEndpoint();

    /// <summary>
    /// The rotation this connection string draws from, shared with every other connection string carrying
    /// the same <c>Endpoint=</c> list. Sharing is what makes the rotation and the endpoint health it
    /// depends on mean anything under EF Core, which rebuilds the builder per connection — see
    /// <see cref="CamusEndpointPool"/>.
    /// </summary>
    internal CamusEndpointPool EndpointPool
    {
        get
        {
            if (endpointPool is not null)
                return endpointPool;

            if (!Config.TryGetValue("Endpoint", out string? endpoint) || string.IsNullOrWhiteSpace(endpoint))
                throw new CamusException("CADB0000", "Endpoint is required");

            return endpointPool ??= CamusEndpointPool.Shared(endpoint);
        }
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
        EndpointPool.MarkUnreachable(endpoint);
    }

    /// <summary>
    /// The connection string as given, credentials included. Use <see cref="ToRedactedString"/> for
    /// anything that is logged, printed or reported as diagnostics.
    /// </summary>
    public override string ToString() => connectionString;

    /// <summary>
    /// The connection string with every secret replaced by <c>***</c> — the value of <c>Password</c>,
    /// <c>Pwd</c> and <c>AccessToken</c>. Everything else, including the original spelling and order of
    /// the keys, is left as written, so the result still identifies the connection.
    /// </summary>
    public string ToRedactedString() => Redact(connectionString);

    /// <summary>The secret-bearing keys, by the same case-insensitive matching the parser uses.</summary>
    private static readonly string[] SecretKeys = ["Password", "Pwd", "AccessToken"];

    /// <summary>What a redacted secret is replaced with. Not the empty string: an empty value reads as
    /// "no password was configured", which is a different fact.</summary>
    private const string RedactedValue = "***";

    /// <summary>
    /// Masks the secret values in a connection string. Static and self-contained so a caller holding only
    /// the string — the EF Core options extension reporting its debug info, say — can redact without
    /// building anything.
    /// </summary>
    public static string Redact(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return connectionString ?? "";

        List<Setting> settings;

        try
        {
            settings = EnumerateSettings(connectionString);
        }
        catch (CamusException)
        {
            // An unparseable string cannot be masked reliably, and printing it might disclose the secret
            // it failed to parse. Report nothing rather than guess.
            return RedactedValue;
        }

        System.Text.StringBuilder redacted = new(connectionString.Length);
        int copied = 0;

        foreach (Setting setting in settings)
        {
            if (!IsSecretKey(connectionString, setting))
                continue;

            // Replace the value's text in place, keeping any quotes around it — a quoted secret stays
            // quoted, so the shape of the string is unchanged.
            redacted.Append(connectionString, copied, setting.ValueStart - copied).Append(RedactedValue);
            copied = setting.ValueStart + setting.ValueLength;
        }

        return redacted.Append(connectionString, copied, connectionString.Length - copied).ToString();
    }

    private static bool IsSecretKey(string source, in Setting setting)
    {
        ReadOnlySpan<char> key = source.AsSpan(setting.KeyStart, setting.KeyLength);

        foreach (string secret in SecretKeys)
        {
            if (key.Equals(secret, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    /// <summary>
    /// The connection-string key that waives <see cref="EnsureCredentialsAreNotSentInClear"/>.
    /// </summary>
    public const string AllowInsecureCredentialsKey = "AllowInsecureCredentials";

    /// <summary>
    /// Refuses to carry credentials to an endpoint that cannot protect them.
    ///
    /// <para>A password is posted to <c>/login</c> and a bearer token rides every later request. On an
    /// <c>http://</c> endpoint both cross the network in the clear, and the server's own
    /// <c>CADB0519 InsecureTransport</c> check — which the deployment can switch off — only fires after
    /// the password has already been sent. So the client refuses first, before anything leaves the
    /// process.</para>
    ///
    /// <para>A loopback endpoint is allowed: the traffic never reaches a network, and that is how the
    /// documented local examples and the test suite connect. Everything else must be <c>https://</c>,
    /// unless the connection string sets <c>AllowInsecureCredentials=true</c> — for a deployment whose
    /// endpoint is reached over a private link that terminates TLS elsewhere.</para>
    ///
    /// <para><c>BackupEndpoint=</c> is held to the same rule, and deliberately: the token it receives is
    /// the connection's own, which the backup admin API requires to be superuser-grade. It must name the
    /// same deployment as <c>Endpoint=</c>, since that token is minted by, and only meaningful to, that
    /// deployment.</para>
    /// </summary>
    /// <exception cref="CamusException">CADB0519 when credentials are configured against an endpoint that
    /// is neither <c>https</c> nor loopback.</exception>
    internal void EnsureCredentialsAreNotSentInClear()
    {
        if (!Credentials.IsSet)
            return;

        if (Config.TryGetValue(AllowInsecureCredentialsKey, out string? allow) &&
            bool.TryParse(allow, out bool allowed) && allowed)
            return;

        if (Config.TryGetValue("Endpoint", out string? endpoints) && !string.IsNullOrWhiteSpace(endpoints))
        {
            foreach (string endpoint in endpoints.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
                EnsureEndpointIsSecure(endpoint, "Endpoint");
        }

        if (Config.TryGetValue("BackupEndpoint", out string? backupEndpoint) && !string.IsNullOrWhiteSpace(backupEndpoint))
            EnsureEndpointIsSecure(backupEndpoint, "BackupEndpoint");
    }

    private static void EnsureEndpointIsSecure(string endpoint, string key)
    {
        if (Uri.TryCreate(endpoint, UriKind.Absolute, out Uri? uri) &&
            (uri.IsLoopback || string.Equals(uri.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase)))
            return;

        throw new CamusException(
            CamusAuthErrorCodes.InsecureTransport,
            $"This connection string configures credentials against the {key} '{endpoint}', which is neither https " +
            $"nor loopback. The password and the bearer token would cross the network in the clear. Use https, or set " +
            $"{AllowInsecureCredentialsKey}=true if the endpoint is reached over a link that protects them.");
    }
}
