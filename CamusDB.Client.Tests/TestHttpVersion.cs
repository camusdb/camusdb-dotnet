/**
 * This file is part of CamusDB
 *
 * Coverage for the HttpVersion= connection-string key: how it parses, that it keeps REST transports with
 * different versions apart, and that statements, login and the backup admin API all send the version
 * it asks for. The HTTP calls are faked with Flurl's HttpTest, so no server is needed.
 *
 * The last test is live and opt-in: set CAMUSDB_TEST_HTTP3=true. It needs a server with https_certificate
 * and http3_enabled: true, a certificate the client trusts, the HTTPS port open for UDP, and libmsquic on
 * the client host. The endpoint comes from CAMUSDB_TEST_HTTPS_ENDPOINT, default https://localhost:7141.
 */

using System.Net;
using CamusDB.Client.Transport;
using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestHttpVersion
{
    [Fact]
    public void DefaultsToHttp11WhenKeyAbsent()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db");

        Assert.Equal(HttpVersion.Version11, builder.HttpVersion);
    }

    [Theory]
    [InlineData("1.1", "1.1")]
    [InlineData("2", "2.0")]
    [InlineData("2.0", "2.0")]
    [InlineData("3", "3.0")]
    [InlineData("3.0", "3.0")]
    [InlineData(" 3 ", "3.0")]
    [InlineData("4", "1.1")]
    [InlineData("quic", "1.1")]
    public void ParsesKnownVersionsAndFallsBackToHttp11(string value, string expected)
    {
        CamusConnectionStringBuilder builder = new($"Endpoint=http://localhost:5095;Database=db;HttpVersion={value}");

        Assert.Equal(Version.Parse(expected), builder.HttpVersion);
    }

    [Fact]
    public void TransportsAreNotSharedAcrossHttpVersions()
    {
        CamusConnectionStringBuilder http11 = new("Endpoint=http://localhost:5395;Database=db");
        CamusConnectionStringBuilder http3 = new("Endpoint=http://localhost:5395;Database=db;HttpVersion=3");
        CamusConnectionStringBuilder alsoHttp3 = new("Endpoint=http://localhost:5395;Database=other;HttpVersion=3.0");

        Assert.NotSame(http11.GetTransport(), http3.GetTransport());
        Assert.Same(http3.GetTransport(), alsoHttp3.GetTransport());
    }

    [Theory]
    [InlineData(null, "1.1")]
    [InlineData("2", "2.0")]
    [InlineData("3", "3.0")]
    public async Task StatementsSendTheRequestedVersion(string? value, string expected)
    {
        // A port per case: the transport is shared by endpoint, and HttpTest fakes every call anyway.
        string endpoint = $"http://localhost:{9350 + (value is null ? 0 : int.Parse(value))}";

        using HttpTest httpTest = new();
        httpTest.ForCallsTo(endpoint + "/execute-sql-query").RespondWith(
            """{"status":"ok","columns":[{"name":"n","type":2}],"rows":[]}""");

        string connectionString = $"Endpoint={endpoint};Database=test" + (value is null ? "" : $";HttpVersion={value}");
        using CamusConnection connection = new(new CamusConnectionStringBuilder(connectionString));
        await using CamusCommand command = connection.CreateCamusCommand("SELECT n FROM t");

        await using (await command.ExecuteReaderAsync()) { }

        Assert.Equal(Version.Parse(expected), Assert.Single(httpTest.CallLog).HttpRequestMessage.Version);
    }

    [Fact]
    public async Task LoginSendsTheRequestedVersion()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9360/login").RespondWith(
            """{"status":"ok","token":"t1","expiresInSeconds":600}""");
        httpTest.ForCallsTo("http://localhost:9360/execute-sql-query").RespondWith(
            """{"status":"ok","columns":[{"name":"n","type":2}],"rows":[]}""");

        using CamusConnection connection = new(new CamusConnectionStringBuilder(
            "Endpoint=http://localhost:9360;Database=test;HttpVersion=3;User=app;Password=secret"));
        await using CamusCommand command = connection.CreateCamusCommand("SELECT n FROM t");

        await using (await command.ExecuteReaderAsync()) { }

        Assert.Equal(2, httpTest.CallLog.Count);
        Assert.All(httpTest.CallLog, call => Assert.Equal(HttpVersion.Version30, call.HttpRequestMessage.Version));
    }

    [Fact]
    public async Task BackupAdminApiSendsTheRequestedVersion()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9361/v1/backups").RespondWith("""{"status":"ok","backups":[]}""");

        using CamusConnection connection = new(new CamusConnectionStringBuilder(
            "Endpoint=http://localhost:9361;Database=test;HttpVersion=2"));

        Assert.Empty(await connection.Backups.ListBackupsAsync());

        Assert.Equal(HttpVersion.Version20, Assert.Single(httpTest.CallLog).HttpRequestMessage.Version);
    }

    [Fact]
    public async Task FallbackHandlerSendsAgainAfterAFailedHandshake()
    {
        // The error .NET raises when the server aborts the QUIC handshake. The second attempt goes over TCP.
        ScriptedHandler inner = new(new HttpRequestException(HttpRequestError.SecureConnectionError, "TLS alert"), null);
        using HttpMessageInvoker invoker = new(new Http3FallbackHandler { InnerHandler = inner });

        using HttpResponseMessage response = await invoker.SendAsync(new HttpRequestMessage(HttpMethod.Post, "https://localhost:9363/"), CancellationToken.None);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(2, inner.Attempts);
    }

    [Fact]
    public async Task FallbackHandlerSendsAgainOnlyOnce()
    {
        // An endpoint that is down fails both attempts; the caller gets the second error.
        ScriptedHandler inner = new(
            new HttpRequestException(HttpRequestError.ConnectionError, "first"),
            new HttpRequestException(HttpRequestError.ConnectionError, "second"));
        using HttpMessageInvoker invoker = new(new Http3FallbackHandler { InnerHandler = inner });

        HttpRequestException ex = await Assert.ThrowsAsync<HttpRequestException>(
            () => invoker.SendAsync(new HttpRequestMessage(HttpMethod.Post, "https://localhost:9363/"), CancellationToken.None));

        Assert.Equal("second", ex.Message);
        Assert.Equal(2, inner.Attempts);
    }

    [Fact]
    public async Task FallbackHandlerDoesNotSendAgainAfterTheRequestWentOut()
    {
        // The server may already run a request whose response ended early; a second attempt could run a
        // statement twice.
        ScriptedHandler inner = new(new HttpRequestException(HttpRequestError.ResponseEnded, "ended"), null);
        using HttpMessageInvoker invoker = new(new Http3FallbackHandler { InnerHandler = inner });

        await Assert.ThrowsAsync<HttpRequestException>(
            () => invoker.SendAsync(new HttpRequestMessage(HttpMethod.Post, "https://localhost:9363/"), CancellationToken.None));

        Assert.Equal(1, inner.Attempts);
    }

    /// <summary>Fails or succeeds each attempt in turn: an exception fails it, null returns 200.</summary>
    private sealed class ScriptedHandler(params Exception?[] outcomes) : HttpMessageHandler
    {
        public int Attempts { get; private set; }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Exception? outcome = outcomes[Attempts++];

            return outcome is null ? Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)) : Task.FromException<HttpResponseMessage>(outcome);
        }
    }

    /// <summary>
    /// End to end against a server with <c>http3_enabled</c>. The first request goes over TCP and learns of
    /// HTTP/3 from the <c>Alt-Svc</c> header; the next ones use QUIC, so the server lists a connection with
    /// the protocol <c>HTTP/3</c>.
    /// </summary>
    [Fact]
    public async Task ServerSeesAnHttp3Connection()
    {
        if (!string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_HTTP3"), "true", StringComparison.OrdinalIgnoreCase))
            return;

        bool quicSupported = (OperatingSystem.IsLinux() || OperatingSystem.IsMacOS() || OperatingSystem.IsWindows())
                             && System.Net.Quic.QuicConnection.IsSupported;
        Assert.True(quicSupported, "QUIC is not available on this host; install libmsquic.");

        string endpoint = Environment.GetEnvironmentVariable("CAMUSDB_TEST_HTTPS_ENDPOINT") ?? "https://localhost:7141";

        await using CamusConnection connection = new(new CamusConnectionStringBuilder($"Endpoint={endpoint};Database=test;HttpVersion=3"));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync(ifNotExists: true);

        for (int i = 0; i < 3; i++)
        {
            await using CamusCommand ping = connection.CreateCamusCommand("SELECT 1");
            await ping.ExecuteScalarAsync();
        }

        List<string> protocols = [];

        await using CamusCommand show = connection.CreateCamusCommand("SHOW CONNECTIONS");
        await using (CamusDataReader reader = await show.ExecuteReaderAsync())
        {
            int protocol = reader.GetOrdinal("protocol");

            while (await reader.ReadAsync())
                protocols.Add(reader.GetString(protocol));
        }

        Assert.Contains("HTTP/3", protocols);
    }
}
