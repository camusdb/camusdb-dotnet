/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Identifier quoting for the places that build a statement as text — the migration helpers and the
/// <see cref="CamusDatabaseFacadeExtensions"/> helpers — rather than through the query pipeline.
/// </summary>
internal static class CamusIdentifier
{
    /// <summary>
    /// Quotes an identifier the way <see cref="CamusSqlGenerationHelper"/> does — both go through
    /// <see cref="CamusSqlSyntax.DelimitIdentifier"/>, so the provider has one rule for identifiers, not
    /// several. A backtick inside the name is rejected rather than escaped: CamusDB's lexer trims the
    /// delimiters instead of decoding a doubled backtick, so there is no spelling that would survive, and
    /// emitting one anyway would fail midway through the statement as a parse error naming neither the
    /// table nor the column.
    /// </summary>
    internal static string Delimit(string identifier, string parameterName)
        => CamusSqlSyntax.DelimitIdentifier(identifier, parameterName);
}
