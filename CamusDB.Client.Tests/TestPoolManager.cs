
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Tests;

public class TestPoolManager
{
    public TestPoolManager()
    {
        //FlurlHttp.ConfigureClient("https://localhost:7141", cli => cli.Settings.HttpClientFactory = new UntrustedCertClientFactory());
    }

    // The session-pool types are obsolete no-ops kept for source compatibility with Spanner-shaped code;
    // this test pins that they still compile and that a connection built through them works normally.
#pragma warning disable CS0618
    [Fact]
    public async Task TestCreatePoolManager()
    {
        SessionPoolOptions options = new()
        {
            MinimumPooledSessions = 100,
            MaximumActiveSessions = 200,
        };

        string connectionString = "Endpoint=http://localhost:5095;Database=test";

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
#pragma warning restore CS0618
}
