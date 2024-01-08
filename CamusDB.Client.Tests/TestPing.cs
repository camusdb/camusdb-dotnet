
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Tests;

public class TestPing : BaseTest
{
    public TestPing()
    {

    }

    [Fact]
    public async void TestPingResponse()
    {
        CamusConnection connection = await GetConnection();        

        using CamusCommand cmd = connection.CreatePingCommand();

        Assert.Equal(1, await cmd.ExecuteNonQueryAsync());
    }
}