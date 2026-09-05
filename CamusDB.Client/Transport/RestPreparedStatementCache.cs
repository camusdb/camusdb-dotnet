
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

namespace CamusDB.Client.Transport;

/// <summary>A REST handle plus the binding order the server published with it.</summary>
internal sealed class RestPreparedStatement(string statementId, IReadOnlyList<string> parameterNames)
{
    public string StatementId { get; } = statementId;

    public IReadOnlyList<string> ParameterNames { get; } = parameterNames;
}

/// <summary>
/// The REST transport's registrations, keyed by (endpoint, database, sql).
///
/// <para><b>Why the endpoint is part of the key.</b> A REST handle is node-local and never replicated,
/// and this driver's endpoint pool spreads statements across nodes, so one statement legitimately holds
/// a different handle per node. Keying by endpoint makes that the normal case — each node is registered
/// on first use and reused thereafter — instead of a repeating invalidation that a single shared entry
/// would turn every cross-node request into.</para>
///
/// <para>Entries hold the in-flight <see cref="Task{TResult}"/> rather than the finished handle, so
/// concurrent first executions of the same statement await one registration instead of each spending a
/// handle from the caller's server-side cap.</para>
/// </summary>
internal sealed class RestPreparedStatementCache
{
    /// <summary>
    /// The key is the (endpoint, database, sql) triple itself rather than a joined string. The three
    /// components hash and compare ordinally exactly as the joined form did, and no lookup copies the SQL
    /// into a temporary key — which a warm prepared execution would otherwise do on every statement.
    /// </summary>
    private readonly ConcurrentDictionary<StatementKey, Task<RestPreparedStatement>> statements = new();

    private readonly record struct StatementKey(string Endpoint, string Database, string Sql);

    /// <summary>
    /// Returns the registration for a statement, creating it via <paramref name="prepare"/> if this
    /// endpoint has none. A registration that fails is not cached: the failure is reported to the caller
    /// that provoked it, and the next execution takes a fresh turn rather than inheriting a poisoned entry.
    /// </summary>
    public async Task<RestPreparedStatement> GetOrAddAsync(
        string endpoint, string database, string sql, Func<Task<RestPreparedStatement>> prepare)
    {
        StatementKey key = new(endpoint, database, sql);

        while (true)
        {
            if (statements.TryGetValue(key, out Task<RestPreparedStatement>? existing))
            {
                try
                {
                    return await existing.ConfigureAwait(false);
                }
                catch
                {
                    Remove(key, existing);
                    continue;
                }
            }

            TaskCompletionSource<RestPreparedStatement> promise = new(TaskCreationOptions.RunContinuationsAsynchronously);

            if (!statements.TryAdd(key, promise.Task))
                continue;   // lost the race; the winner's registration is the one to use.

            try
            {
                RestPreparedStatement prepared = await prepare().ConfigureAwait(false);
                promise.SetResult(prepared);
                return prepared;
            }
            catch (Exception ex)
            {
                Remove(key, promise.Task);
                promise.TrySetException(ex);
                _ = promise.Task.Exception;   // observed here; the rethrow below is what callers see.
                throw;
            }
        }
    }

    /// <summary>
    /// Forgets a registration the server has told us it no longer knows, returning the handle so the
    /// caller can decide whether it is still worth closing. Only drops the entry when it is still the one
    /// the caller was using, so a concurrent re-registration that already succeeded is not thrown away by
    /// a straggler reacting to the old one.
    /// </summary>
    public void Invalidate(string endpoint, string database, string sql, RestPreparedStatement stale)
    {
        StatementKey key = new(endpoint, database, sql);

        if (statements.TryGetValue(key, out Task<RestPreparedStatement>? existing)
            && existing.IsCompletedSuccessfully
            && ReferenceEquals(existing.Result, stale))
        {
            Remove(key, existing);
        }
    }

    /// <summary>Removes and returns every endpoint's registration for a statement, for a caller that is
    /// closing it.</summary>
    public IReadOnlyList<(string Endpoint, RestPreparedStatement Statement)> Take(string database, string sql)
    {
        List<(string, RestPreparedStatement)> taken = [];

        foreach (KeyValuePair<StatementKey, Task<RestPreparedStatement>> entry in statements)
        {
            // Component comparison, not a suffix match: the endpoint is whatever the key already holds,
            // so nothing has to be sliced back out of a joined string.
            if (!string.Equals(entry.Key.Database, database, StringComparison.Ordinal) ||
                !string.Equals(entry.Key.Sql, sql, StringComparison.Ordinal))
                continue;

            if (statements.TryRemove(entry.Key, out Task<RestPreparedStatement>? removed) && removed.IsCompletedSuccessfully)
                taken.Add((entry.Key.Endpoint, removed.Result));
        }

        return taken;
    }

    private void Remove(StatementKey key, Task<RestPreparedStatement> expected)
        => statements.TryRemove(new KeyValuePair<StatementKey, Task<RestPreparedStatement>>(key, expected));
}
