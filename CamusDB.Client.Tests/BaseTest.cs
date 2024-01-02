
namespace CamusDB.Client.Tests;

public class BaseTest
{
    protected static CamusConnectionStringBuilder? builder;

    protected static async Task<CamusConnection> GetConnection()
    {
        CamusConnection cmConnection;

        if (builder is not null)
        {
            cmConnection = new(builder);

            await cmConnection.OpenAsync();

            return cmConnection;
        }

        SessionPoolOptions options = new()
        {
            MinimumPooledSessions = 100,
            MaximumActiveSessions = 200,
        };

        string connectionString = $"Endpoint=https://localhost:7141;Database=test";

        SessionPoolManager manager = SessionPoolManager.Create(options);

        builder = new(connectionString)
        {
            SessionPoolManager = manager
        };

        Assert.Equal(builder.SessionPoolManager, manager);

        cmConnection = new(builder);

        await cmConnection.OpenAsync();

        return cmConnection;
    }
}

