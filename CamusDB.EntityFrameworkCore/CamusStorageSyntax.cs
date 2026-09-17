/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// The large-value storage statements that the migration helpers and the
/// <see cref="CamusDatabaseFacadeExtensions"/> helpers both compose, so both emit the same text.
/// </summary>
internal static class CamusStorageSyntax
{
    /// <summary><c>ALTER TABLE `t` REWRITE STORAGE</c>, with a trailing <c>INLINE</c> when <paramref name="inline"/> is set.</summary>
    internal static string RewriteStorage(string table, bool inline, string parameterName)
        => $"ALTER TABLE {CamusIdentifier.Delimit(table, parameterName)} REWRITE STORAGE{(inline ? " INLINE" : "")}";
}
