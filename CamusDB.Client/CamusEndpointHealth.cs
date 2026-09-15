using System.Net.Http;
using System.Net.Sockets;
using Flurl.Http;
using Grpc.Core;

namespace CamusDB.Client;

internal static class CamusEndpointHealth
{
    public static void MarkUnreachableIfTransportFailed(
        CamusConnectionStringBuilder builder,
        string endpoint,
        FlurlHttpException exception)
    {
        if (!string.IsNullOrEmpty(endpoint) && exception.Call.Response is null)
            builder.MarkEndpointUnreachable(endpoint);
    }

    /// <summary>
    /// Marks against the pool directly, for callers that hold one instead of a builder — the transports,
    /// which are shared per deployment and so have no one connection string to report through. The pool
    /// is the same instance either overload reaches, since a builder resolves its own from
    /// <see cref="CamusEndpointPool.Shared"/>.
    /// </summary>
    public static void MarkUnreachableIfTransportFailed(
        CamusEndpointPool endpoints,
        string endpoint,
        FlurlHttpException exception)
    {
        if (!string.IsNullOrEmpty(endpoint) && exception.Call.Response is null)
            endpoints.MarkUnreachable(endpoint);
    }

    /// <summary>
    /// The gRPC counterpart: an endpoint that refused or timed out the connection itself is set aside.
    /// Before this existed the gRPC path translated the failure and moved on, so a killed node was never
    /// quarantined on gRPC and learned routing kept steering every statement at it — measured at ~28,000
    /// connection-refused failures per second from 128 workers for the five seconds after a leader kill.
    /// </summary>
    public static void MarkUnreachableIfTransportFailed(
        CamusEndpointPool endpoints,
        string endpoint,
        RpcException exception)
    {
        if (!string.IsNullOrEmpty(endpoint) && IndicatesEndpointDown(exception))
            endpoints.MarkUnreachable(endpoint);
    }

    /// <summary>
    /// Whether a gRPC failure says the endpoint is down, for the pool's purposes — a wider question
    /// than <see cref="IsEndpointUnreachable"/>. A connection that was cut under a call in flight
    /// (the stream ended prematurely, the socket was reset, the request was aborted) leaves that
    /// call's outcome unknown, so it keeps the generic code; but it is just as good evidence that the
    /// node is gone as a refused connect, and every call routed there before the pool learns it fails
    /// the same way. After a leader kill the batcher's open streams die first and its reconnect is
    /// what finally produces the refused connect: marking only on the latter left two seconds and
    /// ~20,000 failed operations between the two.
    /// </summary>
    public static bool IndicatesEndpointDown(RpcException exception)
    {
        if (IsEndpointUnreachable(exception))
            return true;
        if (exception.StatusCode != StatusCode.Unavailable)
            return false;

        for (Exception? inner = exception.Status.DebugException; inner is not null; inner = inner.InnerException)
        {
            if (inner is HttpIOException or HttpRequestException or System.IO.IOException)
                return true;
        }

        string detail = exception.Status.Detail ?? "";
        return detail.Contains("Error reading next message", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("Error writing", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("response ended prematurely", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("request was aborted", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("Connection reset", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("connection was closed", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Whether a gRPC failure means the endpoint could not be connected to at all, as opposed to a
    /// server that answered with a status, or a call that was sent and then lost. Judged on the
    /// client-side <c>Unavailable</c> shapes the runtime produces before any bytes go out: the socket
    /// was refused, the host could not be resolved or reached, or the call could not be started. A
    /// server-raised <c>Unavailable</c> (a node saying one of its peers did not answer) carries neither
    /// the connect wording nor a connect-class socket error, and is not treated as this endpoint being
    /// down.
    /// </summary>
    public static bool IsEndpointUnreachable(RpcException exception)
    {
        if (exception.StatusCode != StatusCode.Unavailable)
            return false;

        // A call that had been sent and then lost its connection is NOT this: its outcome is unknown,
        // and a caller told "never sent" would retry a commit that may have landed. Run lk5 (2026-09-15)
        // did exactly that when a "Connection reset by peer" under "Error reading next message" was
        // accepted here on the strength of its SocketException: 37 rows ended up carrying commits the
        // client had written off. Only the shapes produced before any bytes went out count.
        string detail = exception.Status.Detail ?? "";
        if (detail.Contains("Error reading next message", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("Error writing", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("response ended prematurely", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("request was aborted", StringComparison.OrdinalIgnoreCase))
            return false;

        for (Exception? inner = exception.Status.DebugException; inner is not null; inner = inner.InnerException)
        {
            if (inner is SocketException socket)
                return socket.SocketErrorCode is SocketError.ConnectionRefused or SocketError.HostUnreachable
                    or SocketError.NetworkUnreachable or SocketError.HostNotFound or SocketError.NoData
                    or SocketError.TryAgain or SocketError.AddressNotAvailable;
            if (inner is HttpRequestException { HttpRequestError: HttpRequestError.NameResolutionError })
                return true;
        }

        return detail.Contains("Error connecting to subchannel", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("Connection refused", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("Name or service not known", StringComparison.OrdinalIgnoreCase)
            || detail.Contains("ConnectTimeout", StringComparison.OrdinalIgnoreCase)
            || (detail.Contains("Error starting gRPC call", StringComparison.OrdinalIgnoreCase)
                && !detail.Contains("reset", StringComparison.OrdinalIgnoreCase));
    }
}
