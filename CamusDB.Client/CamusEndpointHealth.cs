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
}
