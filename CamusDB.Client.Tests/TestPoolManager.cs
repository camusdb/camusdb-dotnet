
using Flurl.Http;

namespace CamusDB.Client.Tests;

public class TestPoolManager
{
    public TestPoolManager()
    {
        FlurlHttp.ConfigureClient("https://localhost:7141", cli => cli.Settings.HttpClientFactory = new UntrustedCertClientFactory());
    }

    [Fact]
    public async Task TestCreatePoolManager()
    {
        SessionPoolOptions options = new()
        {
            MinimumPooledSessions = 100,
            MaximumActiveSessions = 200,
        };

        string connectionString = $"Endpoint=https://localhost:7141;Database=test";

        SessionPoolManager manager = SessionPoolManager.Create(options);

        CamusConnectionStringBuilder builder = new(connectionString)
        {
            SessionPoolManager = manager
        };

        Assert.Equal(builder.SessionPoolManager, manager);

        CamusConnection connection = new(builder);

        await connection.OpenAsync();

        CamusPingCommand ping = connection.CreatePingCommand();

        Assert.Equal(1, await ping.ExecuteNonQueryAsync());
    }
}
