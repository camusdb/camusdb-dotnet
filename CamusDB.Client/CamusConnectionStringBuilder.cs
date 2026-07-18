
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client.Transport;

namespace CamusDB.Client;

/// <summary>
/// Represents a connection builder class
/// </summary>
public class CamusConnectionStringBuilder
{
    private readonly string connectionString;

    public SessionPoolManager? SessionPoolManager { get; set; }

    public Dictionary<string, string> Config { get; } = new();

    private CamusEndpointPool? endpointPool;

    private ICamusTransport? transport;

    private readonly object transportLock = new();

    public CamusConnectionStringBuilder(string connectionString)
    {
        this.connectionString = connectionString;

        if (string.IsNullOrWhiteSpace(connectionString))
            return;

        string[] settings = connectionString.Split(";");        

        foreach (string setting in settings)
        {
            string[] varParts = setting.Split("=", 2);
            if (varParts.Length != 2)
                continue;

            Config.TryAdd(varParts[0], varParts[1]);
        }
    }

    /// <summary>
    /// Command timeout in seconds. Read from the "Timeout" key in the connection string.
    /// Defaults to 10 seconds.
    /// </summary>
    public int CommandTimeout
    {
        get
        {
            if (Config.TryGetValue("Timeout", out string? raw) && int.TryParse(raw, out int seconds) && seconds > 0)
                return seconds;
            return 10;
        }
    }

    /// <summary>
    /// Connection-wide default concurrency options parsed from the <c>IsolationLevel=</c>,
    /// <c>TransactionMode=</c> and <c>Locking=</c> connection-string keys (case-insensitive values). Any
    /// key that is absent or unrecognized leaves the corresponding knob <see langword="null"/> (server
    /// default). A per-transaction <see cref="CamusTransactionOptions"/> overrides these.
    /// </summary>
    public CamusTransactionOptions DefaultTransactionOptions => new()
    {
        IsolationLevel = ParseEnum<CamusIsolationLevel>("IsolationLevel"),
        Mode = ParseEnum<CamusTransactionMode>("TransactionMode"),
        Locking = ParseEnum<CamusLocking>("Locking"),
    };

    private T? ParseEnum<T>(string key) where T : struct, Enum
        => Config.TryGetValue(key, out string? raw) && Enum.TryParse(raw, ignoreCase: true, out T value)
            ? value
            : null;

    /// <summary>
    /// The wire protocol this connection speaks, from the <c>Protocol=</c> connection-string key
    /// (case-insensitive: <c>rest</c> or <c>grpc</c>). Absent or unrecognized values default to
    /// <see cref="CamusProtocol.Rest"/>. When <see cref="CamusProtocol.Grpc"/> is selected, the
    /// <c>Endpoint=</c> must address the server's gRPC port.
    /// </summary>
    public CamusProtocol Protocol => ParseEnum<CamusProtocol>("Protocol") ?? CamusProtocol.Rest;

    /// <summary>
    /// The transport this builder's connections use, chosen once from <see cref="Protocol"/> and cached
    /// for the builder's lifetime (a gRPC transport pools long-lived channels, so it must be shared, not
    /// recreated per call).
    /// </summary>
    internal ICamusTransport GetTransport()
    {
        if (transport is not null)
            return transport;

        lock (transportLock)
            return transport ??= Protocol == CamusProtocol.Grpc ? new GrpcTransport() : new RestTransport(this);
    }

    internal string GetEndpoint()
    {
        if (!Config.TryGetValue("Endpoint", out string? endpoint) || string.IsNullOrWhiteSpace(endpoint))
            throw new CamusException("CADB0000", "Endpoint is required");

        endpointPool ??= new CamusEndpointPool(endpoint);

        return endpointPool.GetNextEndpoint();
    }

    internal void MarkEndpointUnreachable(string endpoint)
    {
        endpointPool?.MarkUnreachable(endpoint);
    }

    public override string ToString() => connectionString;
}
