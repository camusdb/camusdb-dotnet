using Flurl.Http;

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
}
