
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
    public async void TestCreateTable()
    {
        CamusConnection connection = await GetConnection();

        string sql = "CREATE TABLE test_" + Guid.NewGuid().ToString("n") + " ( id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, status INT64 NOT NULL)";

        using CamusCommand cmd = connection.CreateCamusCommand(sql);        

        Assert.True(await cmd.ExecuteDDLAsync());
    }
}