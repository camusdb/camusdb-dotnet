/**
 * This file is part of CamusDB
 *
 * Offline coverage for transport selection: the `Protocol=` connection-string key picks REST (default)
 * or gRPC, case-insensitively, and the builder hands out a single cached transport of that protocol.
 * No server is required — nothing here opens a connection.
 */

using CamusDB.Client.Transport;

namespace CamusDB.Client.Tests;

public class TestProtocolSelection
{
    [Fact]
    public void DefaultsToRestWhenKeyAbsent()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db");

        Assert.Equal(CamusProtocol.Rest, builder.Protocol);
        Assert.Equal(CamusProtocol.Rest, builder.GetTransport().Protocol);
    }

    [Theory]
    [InlineData("grpc")]
    [InlineData("Grpc")]
    [InlineData("GRPC")]
    public void SelectsGrpcCaseInsensitively(string value)
    {
        CamusConnectionStringBuilder builder = new($"Endpoint=http://localhost:5096;Database=db;Protocol={value}");

        Assert.Equal(CamusProtocol.Grpc, builder.Protocol);
        Assert.Equal(CamusProtocol.Grpc, builder.GetTransport().Protocol);
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("REST")]
    public void SelectsRestExplicitly(string value)
    {
        CamusConnectionStringBuilder builder = new($"Endpoint=http://localhost:5095;Database=db;Protocol={value}");

        Assert.Equal(CamusProtocol.Rest, builder.Protocol);
        Assert.Equal(CamusProtocol.Rest, builder.GetTransport().Protocol);
    }

    [Fact]
    public void UnrecognizedProtocolFallsBackToRest()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5095;Database=db;Protocol=carrierpigeon");

        Assert.Equal(CamusProtocol.Rest, builder.Protocol);
    }

    [Fact]
    public void TransportIsCachedPerBuilder()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=http://localhost:5096;Database=db;Protocol=grpc");

        ICamusTransport first = builder.GetTransport();
        ICamusTransport second = builder.GetTransport();

        Assert.Same(first, second);
    }

    [Fact]
    public void ConnectionsToOneDeploymentShareOneTransport()
    {
        // EF rebuilds the connection-string builder per DbConnection. A transport per builder meant a
        // prepared-statement registration cache per DbContext: the shared auto-prepare policy reports a
        // hot statement as already prepared, so every new transport registered it again and nothing closed
        // the ones it replaced, until the server's per-principal cap refused them (CADB0521).
        CamusConnectionStringBuilder first = new("Endpoint=http://localhost:5195;Database=one");
        CamusConnectionStringBuilder second = new("Endpoint=http://localhost:5195;Database=two");

        Assert.Same(first.GetTransport(), second.GetTransport());
    }

    [Fact]
    public void TransportsAreNotSharedAcrossDeploymentsOrIdentities()
    {
        CamusConnectionStringBuilder rest = new("Endpoint=http://localhost:5295;Database=db");
        CamusConnectionStringBuilder otherEndpoint = new("Endpoint=http://localhost:5296;Database=db");
        CamusConnectionStringBuilder otherProtocol = new("Endpoint=http://localhost:5295;Database=db;Protocol=grpc");
        CamusConnectionStringBuilder otherUser = new("Endpoint=http://localhost:5295;Database=db;User=someone;Password=x");
        CamusConnectionStringBuilder otherPoolSize = new("Endpoint=http://localhost:5295;Database=db;ChannelPoolSize=8");

        ICamusTransport transport = rest.GetTransport();

        Assert.NotSame(transport, otherEndpoint.GetTransport());
        Assert.NotSame(transport, otherProtocol.GetTransport());
        Assert.NotSame(transport, otherUser.GetTransport());
        Assert.NotSame(transport, otherPoolSize.GetTransport());
    }
}
