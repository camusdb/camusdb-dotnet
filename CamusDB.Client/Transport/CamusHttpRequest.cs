/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Net;
using Flurl.Http;
using Flurl.Http.Configuration;

namespace CamusDB.Client.Transport;

/// <summary>
/// Starts every request the driver sends to the server's HTTP API, at the HTTP version the connection
/// string asks for with <c>HttpVersion=</c>.
///
/// <para><b>Why the version must be set.</b> Flurl sends HTTP/1.1 unless told otherwise, and the .NET
/// handler acts on the request version alone: it negotiates HTTP/2 only for a 2.0 request, and it follows
/// the server's <c>Alt-Svc</c> header to HTTP/3 only for a 3.0 request. A server with
/// <c>http3_enabled</c> therefore never sees QUIC from a client that keeps the default.</para>
///
/// <para><b>Fallback.</b> The request keeps the default <see cref="HttpVersionPolicy.RequestVersionOrLower"/>,
/// so the version is a ceiling, not a demand. A 3.0 request to an endpoint the handler has no
/// <c>Alt-Svc</c> entry for goes over TCP; the response advertises HTTP/3 and the next requests to that
/// endpoint use QUIC. A host without QUIC or a plain <c>http://</c> endpoint leaves the request on HTTP/2
/// or HTTP/1.1 with no error. A 2.0 request over plain <c>http://</c> stays on HTTP/1.1, because the
/// handler does not upgrade to cleartext HTTP/2 under that policy.</para>
///
/// <para><b>A failed QUIC connection.</b> When the .NET handler tries QUIC and the connection fails — for
/// example, the server aborts the TLS handshake — the request that tried it fails, and the handler then
/// stops using HTTP/3 for that endpoint. HTTP/3 requests therefore go through their own client, whose
/// <see cref="Http3FallbackHandler"/> sends such a request again over TCP, so the caller does not see the
/// failure.</para>
///
/// <para>HTTP/1.1 leaves the request untouched, so the default sends the same request as before the key
/// existed. HTTP/2 needs no client of its own: only the request version changes.</para>
/// </summary>
internal static class CamusHttpRequest
{
    /// <summary>One client for every HTTP/3 request in the process, so the endpoints it learned to reach
    /// over QUIC, and the ones it stopped trying, are shared by all connections.</summary>
    private static readonly Lazy<IFlurlClient> Http3Client = new(static () => new FlurlClientBuilder()
        .WithSettings(static settings => settings.HttpVersion = "3.0")
        .AddMiddleware(static () => new Http3FallbackHandler())
        .Build());

    public static IFlurlRequest Start(string endpoint, Version httpVersion)
    {
        if (httpVersion == HttpVersion.Version30)
            return Http3Client.Value.Request(endpoint);

        FlurlRequest request = new(endpoint);

        if (httpVersion != HttpVersion.Version11)
            request.Settings.HttpVersion = httpVersion.ToString(2);

        return request;
    }
}
