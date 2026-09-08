/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;

namespace CamusDB.Client;

/// <summary>Whether routing advice asks the client to remember or to forget a destination.</summary>
public enum CamusRoutingDisposition
{
    /// <summary>The server sent a disposition this driver version does not know. The advice is ignored.</summary>
    Unknown = 0,

    /// <summary>Remember the advertised node for future executions of this statement context.</summary>
    Prefer = 1,

    /// <summary>Forget any previously learned destination for this statement context.</summary>
    Clear = 2,
}

/// <summary>
/// Advisory routing metadata a CamusDB server attaches to a successful SQL response when the
/// request negotiated it (<c>routingAcceptVersion = 1</c>). It names the node that leads the
/// statement's data, so a multi-endpoint connection can send the statement's future executions
/// there directly and skip a forwarding hop.
///
/// <para><b>Advice, never an instruction.</b> The statement that carried it already executed
/// normally; ignoring the advice loses nothing but a network hop, and it changes no result,
/// isolation, or commit behavior. The driver has already applied it to its learned-route state by
/// the time a caller sees it, so this type exists for diagnostics — surfaced on
/// <see cref="CamusCommand.LastRoutingAdvice"/> after an execution that negotiated it.</para>
/// </summary>
public sealed class CamusRoutingAdvice
{
    /// <summary>The only metadata version this driver requests and accepts.</summary>
    internal const int AcceptVersion = 1;

    /// <summary>Metadata contract version the server reported; advice with an unknown version is ignored.</summary>
    public int Version { get; }

    /// <summary>Remember or forget — see <see cref="CamusRoutingDisposition"/>.</summary>
    public CamusRoutingDisposition Disposition { get; }

    /// <summary>
    /// Opaque node identity to prefer, or null on a clear. The driver maps it through the
    /// operator-configured <c>RoutingNodes=</c> trust map; it is never an address to dial directly.
    /// </summary>
    public string? PreferredNodeId { get; }

    /// <summary>
    /// True when the server explicitly allowed reuse across parameter values
    /// (<c>reuseScope = "statementParametersIndependent"</c>). An unknown scope is never treated
    /// as parameter-independent — the advice is simply not learned.
    /// </summary>
    public bool ParametersIndependentScope { get; }

    /// <summary>Opaque change detector over the statement's resolved dependencies. Not sortable.</summary>
    public string? DependencyToken { get; }

    /// <summary>Maximum reuse period, measured monotonically from receipt and clamped by driver policy.</summary>
    public int MaxAgeMs { get; }

    /// <summary>"singleTableHash", "ineligible", "placementUnknown", or "cacheAffinity".</summary>
    public string? Reason { get; }

    internal CamusRoutingAdvice(
        int version,
        CamusRoutingDisposition disposition,
        string? preferredNodeId,
        bool parametersIndependentScope,
        string? dependencyToken,
        int maxAgeMs,
        string? reason)
    {
        Version = version;
        Disposition = disposition;
        PreferredNodeId = preferredNodeId;
        ParametersIndependentScope = parametersIndependentScope;
        DependencyToken = dependencyToken;
        MaxAgeMs = maxAgeMs;
        Reason = reason;
    }

    /// <summary>
    /// Reads the optional <c>routing</c> object off a REST query response's DOM, or returns
    /// <see langword="null"/> when the response carried none — every request that did not
    /// negotiate, and every statement that produced no advice. Follows the same
    /// check-the-kind-before-reading discipline as <see cref="CamusCacheMetadata.FromJson"/>.
    /// </summary>
    internal static CamusRoutingAdvice? FromJson(JsonElement root)
    {
        if (!root.TryGetProperty("routing", out JsonElement routing) || routing.ValueKind != JsonValueKind.Object)
            return null;

        int version = routing.TryGetProperty("version", out JsonElement v)
            && v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out int vv) ? vv : 0;

        int maxAgeMs = routing.TryGetProperty("maxAgeMs", out JsonElement age)
            && age.ValueKind == JsonValueKind.Number && age.TryGetInt32(out int a) ? a : 0;

        return new CamusRoutingAdvice(
            version,
            ParseDisposition(ReadString(routing, "disposition")),
            ReadString(routing, "preferredNodeId"),
            string.Equals(ReadString(routing, "reuseScope"), "statementParametersIndependent", StringComparison.Ordinal),
            ReadString(routing, "dependencyToken"),
            maxAgeMs,
            ReadString(routing, "reason"));
    }

    /// <summary>Builds advice from the typed REST DTO the non-query path deserializes. Null in, null out.</summary>
    internal static CamusRoutingAdvice? FromResponse(CamusRoutingMetadataResponse? routing)
    {
        if (routing is null)
            return null;

        return new CamusRoutingAdvice(
            routing.Version,
            ParseDisposition(routing.Disposition),
            routing.PreferredNodeId,
            string.Equals(routing.ReuseScope, "statementParametersIndependent", StringComparison.Ordinal),
            routing.DependencyToken,
            routing.MaxAgeMs,
            routing.Reason);
    }

    /// <summary>
    /// Builds advice from the gRPC message carried by a query/non-query terminator. The server
    /// emits it only for a request that negotiated, so an absent message maps to
    /// <see langword="null"/>. Unknown enum values survive as <see cref="CamusRoutingDisposition.Unknown"/>
    /// or a false scope flag rather than being guessed at, so the learner ignores what it does not
    /// understand.
    /// </summary>
    internal static CamusRoutingAdvice? FromProto(Grpc.RoutingAdvice? advice)
    {
        if (advice is null)
            return null;

        CamusRoutingDisposition disposition = advice.Disposition switch
        {
            Grpc.RoutingDisposition.Prefer => CamusRoutingDisposition.Prefer,
            Grpc.RoutingDisposition.Clear => CamusRoutingDisposition.Clear,
            _ => CamusRoutingDisposition.Unknown,
        };

        return new CamusRoutingAdvice(
            advice.Version,
            disposition,
            advice.PreferredNodeId.Length > 0 ? advice.PreferredNodeId : null,
            advice.ReuseScope == Grpc.RoutingReuseScope.StatementParametersIndependent,
            advice.DependencyToken.Length > 0 ? advice.DependencyToken : null,
            advice.MaxAgeMs,
            advice.Reason.Length > 0 ? advice.Reason : null);
    }

    private static CamusRoutingDisposition ParseDisposition(string? disposition) => disposition switch
    {
        "prefer" => CamusRoutingDisposition.Prefer,
        "clear" => CamusRoutingDisposition.Clear,
        _ => CamusRoutingDisposition.Unknown,
    };

    private static string? ReadString(JsonElement element, string property)
        => element.TryGetProperty(property, out JsonElement value) && value.ValueKind == JsonValueKind.String
            ? value.GetString()
            : null;
}
