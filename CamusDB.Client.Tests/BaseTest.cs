
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

