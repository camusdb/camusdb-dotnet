
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

    [Fact]
    public void TestConnectionStringEndpointPoolRoundRobin()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082,http://localhost:8084,http://localhost:8086;Database=test")
        {

        };

        Assert.Equal("http://localhost:8082,http://localhost:8084,http://localhost:8086", builder.Config["Endpoint"]);
        Assert.Equal("http://localhost:8082", builder.GetEndpoint());
        Assert.Equal("http://localhost:8084", builder.GetEndpoint());
        Assert.Equal("http://localhost:8086", builder.GetEndpoint());
        Assert.Equal("http://localhost:8082", builder.GetEndpoint());
    }

    [Fact]
    public void TestConnectionStringEndpointPoolSkipsUnreachableEndpoints()
    {
        // A pool is shared by every builder carrying the same Endpoint= list, so a test that advances the
        // rotation or quarantines a node needs an endpoint list no other test draws from.
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8182,http://localhost:8184,http://localhost:8186;Database=test")
        {

        };

        Assert.Equal("http://localhost:8182", builder.GetEndpoint());

        builder.MarkEndpointUnreachable("http://localhost:8184");

        Assert.Equal("http://localhost:8186", builder.GetEndpoint());
        Assert.Equal("http://localhost:8182", builder.GetEndpoint());
        Assert.Equal("http://localhost:8186", builder.GetEndpoint());
    }

    [Fact]
    public void TestConnectionStringBatchOptionsDefaults()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:8082;Database=test");

        Assert.Equal(2, builder.BatchOptions.ChannelPoolSize);
        Assert.Equal(10, builder.BatchOptions.CoalescingThreshold);
        Assert.Equal(2, builder.BatchOptions.CoalescingDelayMs);
    }

    [Fact]
    public void TestConnectionStringBatchOptionsOverridden()
    {
        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:8082;Database=test;ChannelPoolSize=8;CoalescingThreshold=32;CoalescingDelay=0");

        Assert.Equal(8, builder.BatchOptions.ChannelPoolSize);
        Assert.Equal(32, builder.BatchOptions.CoalescingThreshold);
        Assert.Equal(0, builder.BatchOptions.CoalescingDelayMs);
    }

    [Fact]
    public void TestConnectionStringBatchOptionsIgnoresOutOfRangeValues()
    {
        CamusConnectionStringBuilder builder = new(
            "Endpoint=http://localhost:8082;Database=test;ChannelPoolSize=0;CoalescingThreshold=nope;CoalescingDelay=-1");

        Assert.Equal(2, builder.BatchOptions.ChannelPoolSize);
        Assert.Equal(10, builder.BatchOptions.CoalescingThreshold);
        Assert.Equal(2, builder.BatchOptions.CoalescingDelayMs);
    }
}
