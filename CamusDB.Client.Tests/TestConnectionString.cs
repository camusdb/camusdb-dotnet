
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Tests;

public class TestConnectionString : BaseTest
{
	public TestConnectionString()
	{
	}

    [Fact]
    public void TestConnectionStringEmpty()
    {
        CamusConnectionStringBuilder builder = new("")
        {
            
        };

        Assert.Empty(builder.Config);
    }

    [Fact]
    public void TestConnectionStringWrongVar()
    {
        CamusConnectionStringBuilder builder = new("a")
        {

        };

        Assert.Empty(builder.Config);
    }

    [Fact]
    public void TestConnectionStringOneVariable()
    {
        CamusConnectionStringBuilder builder = new($"Endpoint=https://localhost:7141")
        {

        };

        Assert.Single(builder.Config);

        Assert.Equal("https://localhost:7141", builder.Config["Endpoint"]);
    }

    [Fact]
    public void TestConnectionStringTwoVariables()
    {
        CamusConnectionStringBuilder builder = new($"Endpoint=https://localhost:7141;Database=test")
        {

        };

        Assert.NotEmpty(builder.Config);

        Assert.Equal("https://localhost:7141", builder.Config["Endpoint"]);
        Assert.Equal("test", builder.Config["Database"]);
    } 
}