namespace CamusDB.Client;

internal sealed class CamusEndpointPool
{
    private readonly string[] endpoints;

    private readonly bool[] unreachableEndpoints;

    private readonly object sync = new();

    private int nextEndpointIndex;

    public CamusEndpointPool(string endpointConfig)
    {
        endpoints = endpointConfig
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        unreachableEndpoints = new bool[endpoints.Length];
    }

    public string GetNextEndpoint()
    {
        lock (sync)
        {
            for (int i = 0; i < endpoints.Length; i++)
            {
                int endpointIndex = nextEndpointIndex;
                nextEndpointIndex = (nextEndpointIndex + 1) % endpoints.Length;

                if (!unreachableEndpoints[endpointIndex])
                    return endpoints[endpointIndex];
            }
        }

        throw new CamusException("CADB0000", "No reachable CamusDB endpoints are available");
    }

    public void MarkUnreachable(string endpoint)
    {
        lock (sync)
        {
            for (int i = 0; i < endpoints.Length; i++)
            {
                if (string.Equals(endpoints[i], endpoint, StringComparison.OrdinalIgnoreCase))
                {
                    unreachableEndpoints[i] = true;
                    return;
                }
            }
        }
    }
}
