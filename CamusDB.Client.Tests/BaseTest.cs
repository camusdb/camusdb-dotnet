
namespace CamusDB.Client.Tests;

public class BaseTest
{
    protected static async Task<CamusConnection> GetConnection()
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

        CamusConnection cmConnection = new(builder);

        await cmConnection.OpenAsync();

        return cmConnection;
    }
}

