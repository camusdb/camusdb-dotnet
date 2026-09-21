/**
 * This file is part of CamusDB
 *
 * Offline coverage for how the real gRPC batch transport learns that its stream's server reads frames:
 * from the stream's response headers, without ever blocking on them. Driven by a fake CallInvoker, so no
 * server is needed.
 */

using CamusDB.Client.Transport.Batching;
using CamusDB.Grpc;
using Grpc.Core;

namespace CamusDB.Client.Tests;

public class TestGrpcBatchTransportAnnouncement
{
    [Fact]
    public async Task FramesAreAnnouncedOnlyOnceTheHeaderArrives()
    {
        TaskCompletionSource<global::Grpc.Core.Metadata> headers = new();
        await using GrpcBatchTransport transport = new(1, new CamusSql.CamusSqlClient(new FakeInvoker(headers.Task)));

        // Constructing the transport did not wait for the headers, and nothing is assumed meanwhile.
        Assert.False(transport.FramesAnnounced);

        headers.SetResult(new global::Grpc.Core.Metadata { { BatchFrames.HeaderName, "1" } });

        Assert.True(await EventuallyAsync(() => transport.FramesAnnounced));
    }

    [Fact]
    public async Task AServerBuiltBeforeFramesAnnouncesNothing()
    {
        Task<global::Grpc.Core.Metadata> headers = Task.FromResult(new global::Grpc.Core.Metadata { { "content-type", "application/grpc" } });
        await using GrpcBatchTransport transport = new(1, new CamusSql.CamusSqlClient(new FakeInvoker(headers)));

        await Task.Delay(50);
        Assert.False(transport.FramesAnnounced);
    }

    [Fact]
    public async Task AStreamThatFailsBeforeItsHeadersAnnouncesNothing()
    {
        Task<global::Grpc.Core.Metadata> headers = Task.FromException<global::Grpc.Core.Metadata>(new RpcException(new Status(StatusCode.Unavailable, "down")));
        await using GrpcBatchTransport transport = new(1, new CamusSql.CamusSqlClient(new FakeInvoker(headers)));

        await Task.Delay(50);
        Assert.False(transport.FramesAnnounced);
    }

    [Fact]
    public async Task ATransportThatAcceptsResponseFramesSaysSoInTheRequestMetadata()
    {
        FakeInvoker invoker = new(Task.FromResult(new global::Grpc.Core.Metadata()));
        global::Grpc.Core.Metadata authentication = new() { { "authorization", "Bearer token" } };

        await using GrpcBatchTransport transport = new(1, new CamusSql.CamusSqlClient(invoker), authentication, acceptResponseFrames: true);

        Assert.NotNull(invoker.RequestHeaders);
        Assert.Equal(BatchFrames.Version.ToString(), invoker.RequestHeaders.GetValue(BatchFrames.AcceptHeaderName));
        // The authentication travelled with it, and the caller's own metadata was not touched.
        Assert.Equal("Bearer token", invoker.RequestHeaders.GetValue("authorization"));
        Assert.Single(authentication);
    }

    [Fact]
    public async Task ATransportWithFramesOffSendsNoAcceptHeader()
    {
        FakeInvoker invoker = new(Task.FromResult(new global::Grpc.Core.Metadata()));

        await using GrpcBatchTransport transport = new(1, new CamusSql.CamusSqlClient(invoker), new global::Grpc.Core.Metadata());

        Assert.NotNull(invoker.RequestHeaders);
        Assert.Null(invoker.RequestHeaders.GetValue(BatchFrames.AcceptHeaderName));
    }

    private static async Task<bool> EventuallyAsync(Func<bool> condition)
    {
        for (int i = 0; i < 200 && !condition(); i++)
            await Task.Delay(10);

        return condition();
    }

    /// <summary>Opens a duplex call that goes nowhere, with response headers the test controls.</summary>
    private sealed class FakeInvoker(Task<global::Grpc.Core.Metadata> responseHeaders) : CallInvoker
    {
        /// <summary>The request metadata the last duplex call opened with.</summary>
        public global::Grpc.Core.Metadata? RequestHeaders { get; private set; }

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options)
        {
            RequestHeaders = options.Headers ?? [];

            return new(
                new NullWriter<TRequest>(), new EmptyReader<TResponse>(), responseHeaders,
                () => Status.DefaultSuccess, () => [], () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
            => throw new NotSupportedException();

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
            => throw new NotSupportedException();

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
            => throw new NotSupportedException();

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method, string? host, CallOptions options)
            => throw new NotSupportedException();
    }

    private sealed class NullWriter<T> : IClientStreamWriter<T>
    {
        public WriteOptions? WriteOptions { get; set; }

        public Task WriteAsync(T message) => Task.CompletedTask;

        public Task CompleteAsync() => Task.CompletedTask;
    }

    private sealed class EmptyReader<T> : IAsyncStreamReader<T>
    {
        public T Current => default!;

        public Task<bool> MoveNext(CancellationToken cancellationToken) => Task.FromResult(false);
    }
}
