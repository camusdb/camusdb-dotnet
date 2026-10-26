/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// What the driver's spans record of the SQL text, as <c>db.query.text</c>. Set with the
/// <c>TraceQueryText=</c> connection-string key.
/// </summary>
public enum CamusQueryTextMode
{
    /// <summary>No SQL text. <c>TraceQueryText=false</c>, and the default.</summary>
    Off = 0,

    /// <summary>The SQL text as it was sent, literal values included. <c>TraceQueryText=true</c>.</summary>
    Raw = 1,

    /// <summary>The SQL text with each literal value replaced by <c>?</c> and each comment removed.
    /// <c>TraceQueryText=Sanitized</c>.</summary>
    Sanitized = 2,
}
