/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// CamusDB statements that have no EF Core API, reached through <c>context.Database</c>.
/// </summary>
public static class CamusDatabaseFacadeExtensions
{
    /// <summary>
    /// Empties a base table with <c>TRUNCATE TABLE</c>. No row is read and no row is deleted: the server
    /// retires the key-space the table's rows and index entries live in, so the cost is the same on an
    /// empty table and on a billion-row one. The table keeps its name, its id, its columns, its indexes
    /// and its grants.
    ///
    /// <para>Reach for this instead of <c>RemoveRange</c> over a whole table, which reads every row into
    /// the change tracker and then deletes them one by one — and which exceeds the per-transaction
    /// mutation limit on any table worth emptying.</para>
    /// </summary>
    /// <param name="database">The context's <see cref="DatabaseFacade"/>.</param>
    /// <param name="name">The table to empty. Quoted for you; it is an identifier, not a parameter.</param>
    /// <remarks>
    /// <para>CamusDB refuses <c>TRUNCATE</c> inside an explicit transaction with <c>CADB0538</c>, because
    /// it commits a replicated schema entry that a <c>ROLLBACK</c> cannot undo. So call this outside
    /// <c>BeginTransaction</c>, and treat it as a step that cannot be taken back with the rest of a unit
    /// of work.</para>
    ///
    /// <para>The previous contents are retained as recoverable retired contents for the server's
    /// retention window; <c>SHOW ORPHAN TABLES</c> lists them and <c>RELINK TO</c> recovers them as a
    /// separate table. The statement reports no row count, so nothing is returned.</para>
    ///
    /// <para>The entities the context already tracks are not affected. Call
    /// <c>ChangeTracker.Clear()</c> if the context outlives the truncate.</para>
    /// </remarks>
    public static void TruncateTable(this DatabaseFacade database, string name)
    {
        ArgumentNullException.ThrowIfNull(database);

        _ = database.ExecuteSqlRaw(TruncateSql(name));
    }

    /// <inheritdoc cref="TruncateTable(DatabaseFacade, string)"/>
    public static async Task TruncateTableAsync(
        this DatabaseFacade database,
        string name,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);

        _ = await database.ExecuteSqlRawAsync(TruncateSql(name), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Converts the rows a table already stores to its current large-value storage rules with
    /// <c>ALTER TABLE … REWRITE STORAGE</c>, or, with <paramref name="inline"/>, back to the inline and
    /// uncompressed form with <c>REWRITE STORAGE INLINE</c>. No value changes, so no query result
    /// changes; only the physical form of each row and the I/O a query does change.
    /// </summary>
    /// <param name="database">The context's <see cref="DatabaseFacade"/>.</param>
    /// <param name="name">The table to rewrite. Quoted for you; it is an identifier, not a parameter.</param>
    /// <param name="inline">
    /// Store every value inside its row, uncompressed. This is the form a server that predates
    /// large-value storage can read.
    /// </param>
    /// <remarks>
    /// <para>The server runs the rewrite in its own bounded transactions, not in the caller's
    /// transaction, so a <c>ROLLBACK</c> does not undo the batches that committed. Call this outside
    /// <c>BeginTransaction</c>.</para>
    ///
    /// <para>The time is proportional to the table. Set a sufficient timeout with
    /// <c>Database.SetCommandTimeout</c> first. The rewrite is resumable: when a run stops, a second run
    /// continues after the last committed batch. A batch that loses to a user write is retried at the end
    /// of the run, and the user write stands.</para>
    /// </remarks>
    public static void RewriteStorage(this DatabaseFacade database, string name, bool inline = false)
    {
        ArgumentNullException.ThrowIfNull(database);

        _ = database.ExecuteSqlRaw(CamusStorageSyntax.RewriteStorage(name, inline, nameof(name)));
    }

    /// <inheritdoc cref="RewriteStorage(DatabaseFacade, string, bool)"/>
    public static async Task RewriteStorageAsync(
        this DatabaseFacade database,
        string name,
        bool inline = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(database);

        _ = await database.ExecuteSqlRawAsync(
            CamusStorageSyntax.RewriteStorage(name, inline, nameof(name)), cancellationToken).ConfigureAwait(false);
    }

    private static string TruncateSql(string name)
        => $"TRUNCATE TABLE {CamusIdentifier.Delimit(name, nameof(name))}";
}
