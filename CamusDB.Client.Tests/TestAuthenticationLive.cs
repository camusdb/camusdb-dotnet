/**
 * This file is part of CamusDB
 *
 * End-to-end authentication against a real server with CAMUSDB_AUTH_ENABLED=true. Opt-in: the whole
 * class no-ops unless CAMUSDB_TEST_USER / CAMUSDB_TEST_PASSWORD are set, because the shared CI instance
 * runs unauthenticated and every other test in this suite depends on that.
 *
 * To run it:
 *   export CAMUSDB_TEST_USER=admin CAMUSDB_TEST_PASSWORD='…'
 *   dotnet test --filter FullyQualifiedName~TestAuthenticationLive
 */

namespace CamusDB.Client.Tests;

/// <summary>
/// Creates the <c>test</c> database once for the whole class. Tests within a class run in an unspecified
/// order, so this cannot live in whichever test happens to need it first.
/// </summary>
public sealed class AuthenticatedDatabaseFixture : IAsyncLifetime
{
    internal static readonly string? User = Environment.GetEnvironmentVariable("CAMUSDB_TEST_USER");
    internal static readonly string? Password = Environment.GetEnvironmentVariable("CAMUSDB_TEST_PASSWORD");

    internal const string Endpoint = "http://localhost:5095";
    internal const string GrpcEndpoint = "http://localhost:5096";

    internal static bool Configured => !string.IsNullOrEmpty(User) && !string.IsNullOrEmpty(Password);

    public async Task InitializeAsync()
    {
        if (!Configured)
            return;

        // Database lifecycle DDL requires the superuser attribute, so this doubles as a check that the
        // configured account really is the bootstrap superuser.
        await using CamusConnection connection = new(
            new CamusConnectionStringBuilder($"Endpoint={Endpoint};Database=test;User={User};Password={Password}"));

        await connection.OpenAsync();
        await connection.CreateDatabaseAsync("test", ifNotExists: true);
    }

    public Task DisposeAsync() => Task.CompletedTask;
}

public class TestAuthenticationLive : IClassFixture<AuthenticatedDatabaseFixture>
{
    private static string? User => AuthenticatedDatabaseFixture.User;
    private static string? Password => AuthenticatedDatabaseFixture.Password;

    private const string Endpoint = AuthenticatedDatabaseFixture.Endpoint;
    private const string GrpcEndpoint = AuthenticatedDatabaseFixture.GrpcEndpoint;

    private static bool Configured => AuthenticatedDatabaseFixture.Configured;

    [Fact]
    public async Task AuthenticatesFromTheConnectionString()
    {
        if (!Configured)
            return;

        await using CamusConnection connection = Connect($"Endpoint={Endpoint};Database=test;User={User};Password={Password}");

        await using CamusCommand command = connection.CreateCamusCommand("SELECT 1 AS one");
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
        Assert.NotNull(connection.AccessToken);
    }

    [Fact]
    public async Task RejectsAnUnauthenticatedConnection()
    {
        if (!Configured)
            return;

        await using CamusConnection connection = Connect($"Endpoint={Endpoint};Database=test");

        await using CamusCommand command = connection.CreateCamusCommand("SELECT 1 AS one");

        CamusException ex = await Assert.ThrowsAsync<CamusException>(() => command.ExecuteReaderAsync());

        Assert.Equal("CADB0516", ex.Code);
    }

    [Fact]
    public async Task RejectsAWrongPassword()
    {
        if (!Configured)
            return;

        await using CamusConnection connection = Connect($"Endpoint={Endpoint};Database=test;User={User};Password=not-the-password");

        CamusException ex = await Assert.ThrowsAsync<CamusException>(
            () => connection.CreateCamusCommand("SELECT 1 AS one").ExecuteReaderAsync());

        Assert.Equal("CADB0516", ex.Code);
    }

    [Fact]
    public async Task LoginAndLogoutManageTheSessionExplicitly()
    {
        if (!Configured)
            return;

        await using CamusConnection connection = Connect($"Endpoint={Endpoint};Database=test");

        string token = await connection.LoginAsync(User!, Password!);
        Assert.StartsWith("camus_", token, StringComparison.Ordinal);

        await using (CamusCommand command = connection.CreateCamusCommand("SELECT 1 AS one"))
        await using (CamusDataReader reader = await command.ExecuteReaderAsync())
            Assert.True(await reader.ReadAsync());

        await connection.LogoutAsync();
        Assert.Null(connection.AccessToken);

        // The credentials survive a logout, so the next statement transparently authenticates again.
        await using (CamusCommand command = connection.CreateCamusCommand("SELECT 1 AS one"))
        await using (CamusDataReader reader = await command.ExecuteReaderAsync())
            Assert.True(await reader.ReadAsync());

        Assert.NotEqual(token, connection.AccessToken);
    }

    [Fact]
    public async Task ReplaysAStatementAfterTheTokenIsRevokedOutOfBand()
    {
        if (!Configured)
            return;

        await using CamusConnection connection = Connect($"Endpoint={Endpoint};Database=test;User={User};Password={Password}");

        await using (CamusCommand warmup = connection.CreateCamusCommand("SELECT 1 AS one"))
        await using (CamusDataReader reader = await warmup.ExecuteReaderAsync())
            Assert.True(await reader.ReadAsync());

        // Revoke server-side while the driver still believes its cached token is good — the same shape as
        // a password rotation or an expiry the driver's clock did not predict.
        string? revoked = connection.AccessToken;
        Assert.NotNull(revoked);
        await RevokeAsync(revoked);

        await using CamusCommand command = connection.CreateCamusCommand("SELECT 1 AS one");
        await using CamusDataReader after = await command.ExecuteReaderAsync();

        Assert.True(await after.ReadAsync());
        Assert.NotEqual(revoked, connection.AccessToken);
    }

    [Fact]
    public async Task AuthenticatesOverGrpcWithoutTheHttpPort()
    {
        // Credential exchange rides the CamusAuth service on the same channel as the statements, so the
        // connection string names only the gRPC endpoint.
        if (!Configured)
            return;

        await using CamusConnection connection = Connect(
            $"Endpoint={GrpcEndpoint};Database=test;Protocol=grpc;User={User};Password={Password}");

        await using CamusCommand command = connection.CreateCamusCommand("SELECT 1 AS one");
        await using CamusDataReader reader = await command.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());
    }

    // Revokes a token the way another process would, without disturbing the driver's cached copy.
    private static async Task RevokeAsync(string token)
    {
        using HttpClient client = new();
        using HttpRequestMessage request = new(HttpMethod.Post, $"{Endpoint}/logout");
        request.Headers.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);

        using HttpResponseMessage response = await client.SendAsync(request);
        response.EnsureSuccessStatusCode();
    }

    private static CamusConnection Connect(string connectionString)
    {
        CamusConnection connection = new(new CamusConnectionStringBuilder(connectionString));
        connection.Open();
        return connection;
    }
}
