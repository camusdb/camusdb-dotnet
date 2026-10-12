/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Transport;

/// <summary>
/// Sends an HTTP/3 request a second time when its first attempt failed before a connection existed.
///
/// <para><b>Why.</b> After an <c>Alt-Svc</c> header, the .NET handler tries QUIC for the next request to
/// that endpoint. If the QUIC connection fails — the server aborts the TLS handshake, or UDP is blocked —
/// that request fails, and only then does the handler stop using HTTP/3 for the endpoint. Without this
/// handler, a working server that advertises a broken HTTP/3 fails one statement per endpoint.</para>
///
/// <para><b>Why a second attempt is safe.</b> It happens only for
/// <see cref="HttpRequestError.ConnectionError"/> and <see cref="HttpRequestError.SecureConnectionError"/>,
/// which .NET raises while it opens a connection, before any byte of the request goes out. The server
/// never saw the request, so a non-idempotent statement does not run twice. The second attempt goes over
/// TCP when the first one failed on QUIC. When the endpoint itself is down, the second attempt fails the
/// same way, and that error goes to the caller.</para>
/// </summary>
internal sealed class Http3FallbackHandler : DelegatingHandler
{
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        try
        {
            return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (HttpRequestException ex) when (ex.HttpRequestError is HttpRequestError.ConnectionError or HttpRequestError.SecureConnectionError)
        {
            return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
    }
}
