
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Tests;

public class TestCreate : BaseTest
{
    public TestCreate()
    {

    }

    [Fact]
    public async Task TestCreateTable()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTempRobotsTableAsync(connection);

        Assert.StartsWith("robots_", tableName, StringComparison.Ordinal);
    }
}
