/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// Error codes the client raises on its own, for conditions no server ever saw. They share the
/// <c>CADB</c> prefix so a caller can switch on one namespace, and sit in the <c>000x</c> range the
/// server reserves for "no domain code" (<c>CADB0000</c>).
/// </summary>
public static class CamusClientErrorCodes
{
    /// <summary>
    /// The request never reached a server: the transport could not connect to the endpoint (connection
    /// refused, no route, connect timeout). Because nothing was sent, the operation is safe to retry —
    /// on another endpoint, which the driver has just quarantined this one in favour of. Distinct from
    /// <c>CADB0000</c>, which also covers a call that was sent and then lost, whose outcome is unknown.
    /// </summary>
    public const string EndpointUnreachable = "CADB0001";
}
