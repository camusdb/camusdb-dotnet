/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// How the query result cache resolved a <c>{cache=…}</c>-hinted <c>SELECT</c>. Parsed from the
/// <c>cacheStatus</c> field the server returns; see the query-result-cache developer guide.
/// </summary>
public enum CamusCacheStatus
{
    /// <summary>The query carried no cache hint (or the server did not report a status).</summary>
    None,

    /// <summary>Stored rows were served from memory without touching storage.</summary>
    Hit,

    /// <summary>The query executed live and a fresh entry was stored.</summary>
    Miss,

    /// <summary>The query executed live and nothing was stored (ineligible, or a write was in flight).</summary>
    Bypass,

    /// <summary>A strict entry was found stale on validation and the query was re-executed live.</summary>
    StaleRevalidated,

    /// <summary>The query returned correct live rows but the fresh entry could not be published.</summary>
    EvictedBeforePublish,

    /// <summary>The server reported a status string this client does not recognize.</summary>
    Unknown,
}
