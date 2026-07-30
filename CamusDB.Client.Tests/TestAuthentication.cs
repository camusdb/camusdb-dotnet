/**
 * This file is part of CamusDB
 *
 * Offline coverage for authentication: credentials parsed out of the connection string, and the token
 * provider's caching / single-flight / renewal behavior against a fake login client. No server is
 * required — nothing here opens a connection.
 */

using CamusDB.Client.Auth;
using CamusDB.Client.Transport;

namespace CamusDB.Client.Tests;

public class TestAuthentication
{
    // ─── Connection-string parsing ────────────────────────────────────────────

    [Fact]
    public void NoCredentialsMeansUnauthenticated()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db");

        Assert.False(builder.Credentials.IsSet);
        Assert.False(builder.TokenProvider.IsEnabled);
        Assert.Null(builder.TokenProvider.CurrentToken);
    }

    [Theory]
    [InlineData("User")]
    [InlineData("UserId")]
    [InlineData("Uid")]
    [InlineData("Username")]
    public void ParsesUserAliases(string key)
    {
        CamusConnectionStringBuilder builder = new($"Endpoint=http://localhost:5095;Database=db;{key}=admin;Password=secret");

        CamusCredentials credentials = builder.Credentials;

        Assert.Equal("admin", credentials.User);
        Assert.Equal("secret", credentials.Password);
        Assert.True(credentials.CanRenew);
    }

    [Fact]
    public void ParsesPasswordAlias()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db;User=admin;Pwd=secret");

        Assert.Equal("secret", builder.Credentials.Password);
    }

    [Fact]
    public void ParsesAccessTokenAndCannotRenewIt()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db;AccessToken=camus_1.abc");

        CamusCredentials credentials = builder.Credentials;

        Assert.Equal("camus_1.abc", credentials.AccessToken);
        Assert.True(credentials.IsSet);
        Assert.False(credentials.CanRenew);
    }

    [Fact]
    public void TokenLifetimeDefaultsAndParses()
    {
        Assert.Equal(
            CamusTokenProvider.DefaultLifetime,
            new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=db").TokenLifetime);

        Assert.Equal(
            TimeSpan.FromSeconds(120),
            new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=db;TokenLifetime=120").TokenLifetime);
    }

    [Fact]
    public void RestConnectionsExchangeCredentialsOverHttp()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db;User=admin;Password=x");

        Assert.IsType<RestTransport>(Assert.IsType<AuthenticatingTransport>(builder.GetTransport()).Inner);
    }

    [Fact]
    public void GrpcConnectionsExchangeCredentialsOverTheirOwnChannel()
    {
        // The CamusAuth service means a gRPC deployment never has to expose the HTTP port just to log in,
        // so there is no second endpoint to configure.
        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:5096;Database=db;Protocol=grpc;User=admin;Password=x");

        ICamusTransport inner = Assert.IsType<AuthenticatingTransport>(builder.GetTransport()).Inner;

        Assert.IsType<GrpcTransport>(inner);
        Assert.IsAssignableFrom<ICamusLoginClient>(inner);
    }

    [Fact]
    public void ConnectionsFromTheSameCredentialsShareOneProvider()
    {
        // EF rebuilds the connection-string builder per DbConnection; a login per connection would hit
        // the server's per-account login rate limit.
        const string credentials = "User=shared-user;Password=shared-secret";

        CamusConnectionStringBuilder first = new($"Endpoint=http://localhost:5095;Database=one;{credentials}");
        CamusConnectionStringBuilder second = new($"Endpoint=http://localhost:5095;Database=two;{credentials}");

        Assert.Same(first.TokenProvider, second.TokenProvider);
    }

    [Fact]
    public void DifferentDeploymentsDoNotShareAProvider()
    {
        const string credentials = "User=shared-user;Password=shared-secret";

        CamusConnectionStringBuilder first = new($"Endpoint=http://host-b:5095;Database=db;{credentials}");
        CamusConnectionStringBuilder second = new($"Endpoint=http://host-c:5095;Database=db;{credentials}");

        Assert.NotSame(first.TokenProvider, second.TokenProvider);
    }

    // ─── Token provider ───────────────────────────────────────────────────────

    [Fact]
    public async Task MintsATokenOnFirstUseAndCachesIt()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"));

        Assert.Equal("token-1", await provider.GetTokenAsync(default));
        Assert.Equal("token-1", await provider.GetTokenAsync(default));

        Assert.Equal(1, login.Logins);
        Assert.Equal(("admin", "secret"), login.LastCredentials);
    }

    [Fact]
    public async Task ReturnsNullWithoutCredentials()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.None);

        Assert.Null(await provider.GetTokenAsync(default));
        Assert.Equal(0, login.Logins);
    }

    [Fact]
    public async Task ConcurrentCallersProduceOneLogin()
    {
        FakeLoginClient login = new() { Gate = new TaskCompletionSource() };
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"));

        Task<string?>[] callers = Enumerable
            .Range(0, 16)
            .Select(_ => provider.GetTokenAsync(default).AsTask())
            .ToArray();

        login.Gate.SetResult();
        string?[] tokens = await Task.WhenAll(callers);

        Assert.All(tokens, token => Assert.Equal("token-1", token));
        Assert.Equal(1, login.Logins);
    }

    [Fact]
    public async Task RenewsAgainstTheServerReportedExpiryRatherThanTheConfiguredLifetime()
    {
        // The server's AccessTokenTtl is configurable and can be far shorter than the driver's fallback;
        // its reported lifetime is authoritative, and the driver renews at 80% of it.
        FakeLoginClient login = new() { ExpiresIn = TimeSpan.FromMinutes(5) };
        TestClock clock = new(DateTimeOffset.UnixEpoch);
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"), TimeSpan.FromMinutes(10), clock);

        Assert.Equal("token-1", await provider.GetTokenAsync(default));

        clock.Advance(TimeSpan.FromMinutes(3));         // 3 min < 80% of 5 min
        Assert.Equal("token-1", await provider.GetTokenAsync(default));

        clock.Advance(TimeSpan.FromMinutes(2));         // 5 min > 4 min, and well past the reported TTL
        Assert.Equal("token-2", await provider.GetTokenAsync(default));
    }

    [Fact]
    public async Task FallsBackToTheConfiguredLifetimeWhenTheServerReportsNoExpiry()
    {
        FakeLoginClient login = new() { ExpiresIn = null };
        TestClock clock = new(DateTimeOffset.UnixEpoch);
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"), TimeSpan.FromMinutes(10), clock);

        Assert.Equal("token-1", await provider.GetTokenAsync(default));

        clock.Advance(TimeSpan.FromMinutes(9));
        Assert.Equal("token-1", await provider.GetTokenAsync(default));

        clock.Advance(TimeSpan.FromMinutes(2));
        Assert.Equal("token-2", await provider.GetTokenAsync(default));
        Assert.Equal(2, login.Logins);
    }

    [Fact]
    public async Task InvalidateForcesAFreshLogin()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"));

        string? first = await provider.GetTokenAsync(default);
        provider.Invalidate(first);

        Assert.Equal("token-2", await provider.GetTokenAsync(default));
        Assert.Equal(2, login.Logins);
    }

    [Fact]
    public async Task InvalidateIgnoresAnAlreadyReplacedToken()
    {
        // Two requests fail concurrently on the same stale token; the second rejection must not discard
        // the replacement the first one already obtained.
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"));

        string? stale = await provider.GetTokenAsync(default);
        provider.Invalidate(stale);
        Assert.Equal("token-2", await provider.GetTokenAsync(default));

        provider.Invalidate(stale);

        Assert.Equal("token-2", await provider.GetTokenAsync(default));
        Assert.Equal(2, login.Logins);
    }

    [Fact]
    public async Task ASuppliedAccessTokenIsUsedVerbatimAndNeverRenewed()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromToken("camus_1.abc"));

        Assert.Equal("camus_1.abc", await provider.GetTokenAsync(default));
        Assert.False(provider.CanRenew);

        provider.Invalidate("camus_1.abc");

        Assert.Equal("camus_1.abc", await provider.GetTokenAsync(default));
        Assert.Equal(0, login.Logins);
    }

    [Fact]
    public async Task ExplicitLoginReplacesTheConfiguredCredentials()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.None);

        Assert.Equal("token-1", await provider.LoginAsync("app", "app-secret", default));
        Assert.Equal(("app", "app-secret"), login.LastCredentials);
        Assert.Equal("token-1", await provider.GetTokenAsync(default));
        Assert.Equal(1, login.Logins);
    }

    [Fact]
    public async Task LogoutRevokesTheTokenAndTheNextStatementReauthenticates()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"));

        Assert.Equal("token-1", await provider.GetTokenAsync(default));

        await provider.LogoutAsync(default);

        Assert.Equal("token-1", login.RevokedToken);
        Assert.Equal("token-2", await provider.GetTokenAsync(default));
    }

    [Fact]
    public async Task LogoutWithoutATokenIsANoOp()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"));

        await provider.LogoutAsync(default);

        Assert.Null(login.RevokedToken);
        Assert.Equal(0, login.Logins);
    }

    // ─── Retry decorator ──────────────────────────────────────────────────────

    [Fact]
    public async Task ARejectedTokenIsRefreshedAndTheStatementReplayed()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"));
        await provider.GetTokenAsync(default);

        FakeTransport inner = new() { FailuresBeforeSuccess = 1, FailureCode = "CADB0516" };
        AuthenticatingTransport transport = new(inner, provider);

        Assert.Equal(7, await transport.ExecuteNonQueryAsync(Request(), default));
        Assert.Equal(2, inner.Calls);

        // The rejected token was discarded, so the next request mints a replacement. (The fake transport
        // does not itself ask for a token; a real one does, on every call.)
        Assert.Equal("token-2", await provider.GetTokenAsync(default));
    }

    [Fact]
    public async Task ReplaysAtMostOnce()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"));
        await provider.GetTokenAsync(default);

        FakeTransport inner = new() { FailuresBeforeSuccess = 5, FailureCode = "CADB0516" };
        AuthenticatingTransport transport = new(inner, provider);

        CamusException ex = await Assert.ThrowsAsync<CamusException>(() => transport.ExecuteNonQueryAsync(Request(), default));

        Assert.Equal("CADB0516", ex.Code);
        Assert.Equal(2, inner.Calls);
    }

    [Fact]
    public async Task InsufficientPrivilegeIsNotReplayed()
    {
        // Re-authenticating as the same user cannot grant a privilege.
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "secret"));
        await provider.GetTokenAsync(default);

        FakeTransport inner = new() { FailuresBeforeSuccess = 1, FailureCode = "CADB0517" };
        AuthenticatingTransport transport = new(inner, provider);

        CamusException ex = await Assert.ThrowsAsync<CamusException>(() => transport.ExecuteNonQueryAsync(Request(), default));

        Assert.Equal("CADB0517", ex.Code);
        Assert.Equal(1, inner.Calls);
    }

    [Fact]
    public async Task ARejectedSuppliedTokenIsNotReplayed()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromToken("camus_1.abc"));

        FakeTransport inner = new() { FailuresBeforeSuccess = 1, FailureCode = "CADB0516" };
        AuthenticatingTransport transport = new(inner, provider);

        await Assert.ThrowsAsync<CamusException>(() => transport.ExecuteNonQueryAsync(Request(), default));

        Assert.Equal(1, inner.Calls);
        Assert.Equal(0, login.Logins);
    }

    [Fact]
    public async Task ARejectedLoginIsNotReplayed()
    {
        // No token was presented, so the rejection came from the login itself — replaying it would burn
        // two attempts per statement against the server's per-account login rate limit.
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.FromPassword("admin", "wrong"));

        FakeTransport inner = new() { FailuresBeforeSuccess = 1, FailureCode = "CADB0516" };
        AuthenticatingTransport transport = new(inner, provider);

        await Assert.ThrowsAsync<CamusException>(() => transport.ExecuteNonQueryAsync(Request(), default));

        Assert.Equal(1, inner.Calls);
    }

    [Fact]
    public async Task UnauthenticatedTransportsPassStraightThrough()
    {
        FakeLoginClient login = new();
        CamusTokenProvider provider = Provider(login, CamusCredentials.None);

        FakeTransport inner = new();
        AuthenticatingTransport transport = new(inner, provider);

        Assert.Equal(7, await transport.ExecuteNonQueryAsync(Request(), default));
        Assert.Equal(1, inner.Calls);
        Assert.Equal(0, login.Logins);
    }

    // ─── Fixtures ─────────────────────────────────────────────────────────────

    private static CamusTokenProvider Provider(
        ICamusLoginClient login, CamusCredentials credentials, TimeSpan? lifetime = null, TimeProvider? clock = null)
        => new(credentials, () => login, () => "http://localhost:5095", () => 10, lifetime, clock);

    private static TransportSqlRequest Request() => new()
    {
        Endpoint = "http://localhost:5095",
        Database = "db",
        Sql = "UPDATE t SET a = 1",
        TimeoutSeconds = 10,
    };

    private sealed class FakeLoginClient : ICamusLoginClient
    {
        private int logins;

        public int Logins => Volatile.Read(ref logins);

        public (string User, string Password)? LastCredentials { get; private set; }

        public string? RevokedToken { get; private set; }

        /// <summary>When set, logins block on it — used to pile callers up on the single-flight gate.</summary>
        public TaskCompletionSource? Gate { get; init; }

        /// <summary>Expiry the fake server reports, or null to model a server predating the field.</summary>
        public TimeSpan? ExpiresIn { get; init; }

        public async Task<CamusLoginResult> LoginAsync(string endpoint, string user, string password, int timeoutSeconds, CancellationToken cancellationToken)
        {
            if (Gate is not null)
                await Gate.Task.ConfigureAwait(false);

            LastCredentials = (user, password);
            return new CamusLoginResult("token-" + Interlocked.Increment(ref logins), ExpiresIn);
        }

        public Task LogoutAsync(string endpoint, string token, int timeoutSeconds, CancellationToken cancellationToken)
        {
            RevokedToken = token;
            return Task.CompletedTask;
        }
    }

    /// <summary>Fails <see cref="FailuresBeforeSuccess"/> times with <see cref="FailureCode"/>, then succeeds.</summary>
    private sealed class FakeTransport : ICamusTransport
    {
        public int Calls { get; private set; }

        public int FailuresBeforeSuccess { get; init; }

        public string FailureCode { get; init; } = "CADB0516";

        public CamusProtocol Protocol => CamusProtocol.Rest;

        public Task<int> ExecuteNonQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
        {
            Calls++;

            if (Calls <= FailuresBeforeSuccess)
                throw new CamusException(FailureCode, "rejected");

            return Task.FromResult(7);
        }

        public Task<StartTransactionResult> StartTransactionAsync(string endpoint, string database, CamusTransactionOptions options, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task FinalizeTransactionAsync(bool commit, string endpoint, string database, long txnIdPT, uint txnIdCounter, int? streamSlot, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<QueryTransportResult> ExecuteQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<CamusRowSource> ExecuteQueryStreamAsync(TransportSqlRequest request, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<bool> ExecuteDdlAsync(TransportSqlRequest request, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<PreparedStatementInfo> PrepareAsync(string endpoint, string database, string sql, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task ClosePreparedAsync(string endpoint, string database, string sql, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<bool> PingAsync(string endpoint, int timeoutSeconds, CancellationToken cancellationToken)
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

    private sealed class TestClock(DateTimeOffset now) : TimeProvider
    {
        private DateTimeOffset utcNow = now;

        public override DateTimeOffset GetUtcNow() => utcNow;

        public void Advance(TimeSpan delta) => utcNow += delta;
    }
}
