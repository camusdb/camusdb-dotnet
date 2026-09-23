/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.IO;
using System.Net.Sockets;
using CamusDB.Client.Transport;
using Grpc.Core;
using Xunit;

namespace CamusDB.Client.Tests;

/// <summary>
/// Pins the gRPC path's handling of an endpoint that cannot be connected to — the case behind the
/// connection-refused storm measured after a leader kill (2026-09-15, Caraxes runs lk1-lk3): the
/// REST transport quarantined such an endpoint, the gRPC transport translated the failure and drew
/// the same endpoint again, and learned routing kept preferring it.
/// </summary>
public class TestEndpointUnreachable
{
    private static RpcException ConnectionRefused() =>
        new(new Status(StatusCode.Unavailable, "Error connecting to subchannel.",
            new SocketException((int)SocketError.ConnectionRefused)));

    private static RpcException ConnectRefusedByDetailOnly() =>
        new(new Status(StatusCode.Unavailable, "Error starting gRPC call. HttpRequestException: Connection refused (camus2:16095)"));

    private static RpcException ServerSaidPeerUnavailable() =>
        new(new Status(StatusCode.Unavailable, "The remote node did not answer within the inter-node request deadline."));

    private static RpcException StreamCutMidCall() =>
        new(new Status(StatusCode.Unavailable, "Error reading next message. HttpIOException: The response ended prematurely while waiting for the next frame from the server."));

    // The exact shape that misled run lk5: a socket error, but on a call that was already in flight.
    private static RpcException ResetWhileReading() =>
        new(new Status(StatusCode.Unavailable,
            "Error reading next message. IOException: The request was aborted. IOException: Unable to read data from the transport connection: Connection reset by peer.",
            new IOException("The request was aborted.", new IOException("Unable to read data from the transport connection: Connection reset by peer.",
                new SocketException((int)SocketError.ConnectionReset)))));

    [Fact]
    public void ConnectFailures_AreClassifiedUnreachable()
    {
        Assert.True(CamusEndpointHealth.IsEndpointUnreachable(ConnectionRefused()));
        Assert.True(CamusEndpointHealth.IsEndpointUnreachable(ConnectRefusedByDetailOnly()));
    }

    [Fact]
    public void ResetOnAnInFlightCall_IsDownButNotNeverSent()
    {
        // The node is gone (quarantine it), but this call may have reached it: keep the unknown-outcome
        // code so a commit is never written off as "never sent".
        Assert.False(CamusEndpointHealth.IsEndpointUnreachable(ResetWhileReading()));
        Assert.True(CamusEndpointHealth.IndicatesEndpointDown(ResetWhileReading()));

        CamusEndpointPool pool = new("http://a:9005,http://b:9005");
        CamusException ex = GrpcTransport.TranslateFailure(pool, "http://b:9005", ResetWhileReading());
        Assert.Equal("CADB0000", ex.Code);
        Assert.True(pool.IsQuarantined("http://b:9005"));
    }

    [Fact]
    public void ServerAnswersAndCutStreams_AreNot()
    {
        Assert.False(CamusEndpointHealth.IsEndpointUnreachable(ServerSaidPeerUnavailable()), "a server that answered is up");
        Assert.False(CamusEndpointHealth.IsEndpointUnreachable(StreamCutMidCall()), "a call that was sent has an unknown outcome, not an unreachable endpoint");
        Assert.False(CamusEndpointHealth.IsEndpointUnreachable(new RpcException(new Status(StatusCode.Internal, "boom"))));
        Assert.False(CamusEndpointHealth.IsEndpointUnreachable(new RpcException(new Status(StatusCode.DeadlineExceeded, "slow"))));
    }

    [Fact]
    public void UnreachableEndpoint_IsQuarantined_AndSurfacedAsRetryable()
    {
        CamusEndpointPool pool = new("http://a:9005,http://b:9005");

        CamusException ex = GrpcTransport.TranslateFailure(pool, "http://b:9005", ConnectionRefused());

        Assert.Equal(CamusClientErrorCodes.EndpointUnreachable, ex.Code);
        Assert.Contains("http://b:9005", ex.Message);
        Assert.True(pool.IsQuarantined("http://b:9005"));
        Assert.False(pool.IsQuarantined("http://a:9005"));
        Assert.Equal("http://a:9005", pool.GetNextEndpoint());
        Assert.Equal("http://a:9005", pool.GetNextEndpoint());
    }

    [Fact]
    public void StreamCutMidCall_QuarantinesTheEndpoint_ButKeepsTheUnknownOutcomeCode()
    {
        CamusEndpointPool pool = new("http://a:9005,http://b:9005");

        Assert.True(CamusEndpointHealth.IndicatesEndpointDown(StreamCutMidCall()));
        Assert.False(CamusEndpointHealth.IndicatesEndpointDown(ServerSaidPeerUnavailable()));

        CamusException ex = GrpcTransport.TranslateFailure(pool, "http://b:9005", StreamCutMidCall());

        Assert.Equal("CADB0000", ex.Code);
        Assert.True(pool.IsQuarantined("http://b:9005"), "the node is gone even though this call's outcome is unknown");
    }

    [Fact]
    public void ServerFailure_LeavesThePoolAlone_AndKeepsTheGenericCode()
    {
        CamusEndpointPool pool = new("http://a:9005,http://b:9005");

        CamusException ex = GrpcTransport.TranslateFailure(pool, "http://b:9005", ServerSaidPeerUnavailable());

        Assert.Equal("CADB0000", ex.Code);
        Assert.False(pool.IsQuarantined("http://b:9005"));
    }

    /// <summary>
    /// The shape a <c>BatchExecute</c> stream now ends with when the token it opened with expires. The
    /// stream is long-lived and carries the bearer it was built with, so the server refuses the next
    /// operation on it; the driver must read that as <c>CADB0516</c>, which is what
    /// <see cref="Transport.AuthenticatingTransport"/> replays on, and must leave the endpoint in
    /// rotation — the node answered, and every stream in the pool reaches this point together.
    /// </summary>
    [Fact]
    public void ExpiredTokenOnAStream_IsAuthFailure_AndLeavesTheEndpointInRotation()
    {
        CamusEndpointPool pool = new("http://a:9005,http://b:9005");
        global::Grpc.Core.Metadata trailers = new()
        {
            { "camus-error-code", "CADB0516" },
            { "camus-error-message", "Authentication failed" },
        };
        RpcException ex = new(new Status(StatusCode.Unauthenticated, "Authentication failed"), trailers);

        CamusException translated = GrpcTransport.TranslateFailure(pool, "http://b:9005", ex);

        Assert.Equal("CADB0516", translated.Code);
        Assert.False(pool.IsQuarantined("http://b:9005"));
    }

    [Fact]
    public void DomainCodeInTrailers_StillWins()
    {
        CamusEndpointPool pool = new("http://a:9005");
        global::Grpc.Core.Metadata trailers = new() { { "camus-error-code", "CADB0504" }, { "camus-error-message", "retry" } };
        RpcException ex = new(new Status(StatusCode.Aborted, "aborted"), trailers);

        CamusException translated = GrpcTransport.TranslateFailure(pool, "http://a:9005", ex);

        Assert.Equal("CADB0504", translated.Code);
        Assert.False(pool.IsQuarantined("http://a:9005"));
    }
}
