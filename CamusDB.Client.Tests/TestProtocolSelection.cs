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
}
