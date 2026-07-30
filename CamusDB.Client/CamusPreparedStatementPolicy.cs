
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>What a command should do with a statement it is about to run.</summary>
internal enum PrepareDecision
{
    /// <summary>Run it inline — not seen often enough, already refused, or auto-preparation is off.</summary>
    No,

    /// <summary>Register it first, then run it prepared. The caller reports the outcome back via
    /// <see cref="CamusPreparedStatementPolicy.MarkPrepared"/> or
    /// <see cref="CamusPreparedStatementPolicy.MarkRefused"/>.</summary>
    Register,

    /// <summary>Already registered — run it prepared.</summary>
    Yes,
}

/// <summary>
/// Decides which statements are worth preparing, for callers that never ask.
///
/// <para>This exists because of how ADO.NET is actually used. <see cref="System.Data.Common.DbCommand.Prepare"/>
/// is the standard way to say "I will run this many times", and essentially nothing calls it — Entity
/// Framework Core never does, on any provider. A driver that only prepares on request therefore never
/// prepares for the workload that would benefit most: an ORM issuing the same handful of statement shapes
/// for the lifetime of a process. So the driver watches instead, and once it has seen the same SQL
/// <see cref="MinUsages"/> times it registers it, exactly as Npgsql's auto-prepare does.</para>
///
/// <para><b>Why a threshold rather than preparing everything.</b> Preparing costs a registration round
/// trip and a slot in a server-side cap that is not this client's alone. A statement executed once — a
/// migration, an ad-hoc query, an EF <c>IN (…)</c> expansion whose text changes with every list length —
/// would pay both and repay neither. Waiting for a repeat separates the two cases without guessing.</para>
///
/// <para><b>Why a cap.</b> Statement text is unbounded in principle: concatenated SQL and expanded
/// parameter lists can produce new text forever, so both the tracking dictionary and the server-side
/// registrations need a ceiling. One LRU ordering covers both — evicting the least recently used
/// statement bounds what this client remembers, and when that statement was registered the caller is
/// handed it so the server stops holding it too. A churning application degrades to inline execution
/// rather than meeting <c>CADB0521</c>.</para>
/// </summary>
internal sealed class CamusPreparedStatementPolicy
{
    /// <summary>
    /// How many distinct statements one connection string tracks, and so at most how many it keeps
    /// registered. Comfortably under the server's default per-principal cap of 512, since several
    /// connection strings — or several processes — may share one principal.
    /// </summary>
    public const int DefaultMaxAutoPrepare = 128;

    /// <summary>
    /// Executions of the same SQL before it is prepared. Two means "the first repeat pays for it": the
    /// statement has proven it is not a one-off, and every execution from the second onward is cheap.
    /// </summary>
    public const int DefaultMinUsages = 2;

    // Policies keyed by the deployment and settings they describe — see Shared().
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, CamusPreparedStatementPolicy> SharedPolicies =
        new(StringComparer.Ordinal);

    /// <summary>
    /// The policy for <paramref name="key"/>, created once per process.
    ///
    /// <para>Sharing is what makes this work for the ORM it exists for. Entity Framework builds a
    /// connection — and with it a connection-string builder — per <c>DbContext</c>, and a web
    /// application's context lives for one request. A policy owned by the builder would therefore start
    /// counting from zero on every request and never reach the threshold, so the statements an
    /// application repeats thousands of times a minute would be exactly the ones never prepared.</para>
    ///
    /// <para>Sharing a <em>decision</em> across builders is safe even though a registration belongs to
    /// one transport: a builder whose transport has not registered a statement the policy already
    /// considers hot simply registers it on first use, which is the round trip the policy was going to
    /// spend anyway.</para>
    /// </summary>
    public static CamusPreparedStatementPolicy Shared(string key, Func<CamusPreparedStatementPolicy> factory)
        => SharedPolicies.GetOrAdd(key, _ => factory());

    private readonly Dictionary<string, Entry> entries = new(StringComparer.Ordinal);

    private readonly LinkedList<Entry> recency = new();

    private readonly object mutex = new();

    private volatile bool disabled;

    public CamusPreparedStatementPolicy(int maxAutoPrepare, int minUsages)
    {
        MaxAutoPrepare = maxAutoPrepare;
        MinUsages = Math.Max(1, minUsages);
        disabled = maxAutoPrepare <= 0;
    }

    /// <summary>Statements tracked, and so at most kept registered, at once. Zero disables automatic
    /// preparation entirely; an explicit <see cref="CamusCommand.Prepare"/> still works.</summary>
    public int MaxAutoPrepare { get; }

    /// <summary>Executions of one statement before it is prepared.</summary>
    public int MinUsages { get; }

    /// <summary>True once the server has shown it cannot prepare at all.</summary>
    public bool IsDisabled => disabled;

    /// <summary>Whether this statement is currently one of the prepared ones.</summary>
    public bool IsPrepared(string database, string sql)
    {
        lock (mutex)
            return entries.TryGetValue(Key(database, sql), out Entry? entry) && entry.State == EntryState.Prepared;
    }

    /// <summary>Statements currently registered. For tests and diagnostics.</summary>
    public int PreparedCount
    {
        get
        {
            lock (mutex)
            {
                int count = 0;
                foreach (Entry entry in recency)
                {
                    if (entry.State == EntryState.Prepared)
                        count++;
                }
                return count;
            }
        }
    }

    /// <summary>
    /// Records an execution of <paramref name="sql"/> and reports what to do with it.
    ///
    /// <para><paramref name="evicted"/> receives a statement that was registered and has now been
    /// forgotten to make room, so the caller can release its server-side handle. Eviction happens here
    /// rather than lazily because the point of the cap is that the server stops holding what this client
    /// stopped using.</para>
    /// </summary>
    public PrepareDecision Decide(string database, string sql, out (string Database, string Sql)? evicted)
    {
        evicted = null;

        if (disabled)
            return PrepareDecision.No;

        lock (mutex)
        {
            Entry entry = Touch(database, sql, ref evicted);

            switch (entry.State)
            {
                case EntryState.Prepared:
                    return PrepareDecision.Yes;

                case EntryState.Refused:
                    return PrepareDecision.No;

                case EntryState.Registering:
                    // A registration is already in flight. Running inline meanwhile is correct rather
                    // than merely tolerable — the statement is not registered yet.
                    return PrepareDecision.No;

                default:
                    if (++entry.Usages < MinUsages)
                        return PrepareDecision.No;

                    entry.State = EntryState.Registering;
                    return PrepareDecision.Register;
            }
        }
    }

    /// <summary>
    /// Asks for a statement to be registered regardless of how often it has run, for a caller that said
    /// so explicitly. Returns <see cref="PrepareDecision.Yes"/> when it already is.
    /// </summary>
    public PrepareDecision Pin(string database, string sql, out (string Database, string Sql)? evicted)
    {
        evicted = null;

        lock (mutex)
        {
            Entry entry = Touch(database, sql, ref evicted);

            // An explicit Prepare() overrides an earlier refusal: the caller may well have fixed whatever
            // made it fail, and it is asking directly rather than being guessed at.
            if (entry.State == EntryState.Prepared)
                return PrepareDecision.Yes;

            entry.State = EntryState.Registering;
            entry.Usages = Math.Max(entry.Usages, MinUsages);
            return PrepareDecision.Register;
        }
    }

    /// <summary>Records that the server registered the statement, so later executions run prepared.</summary>
    public void MarkPrepared(string database, string sql)
    {
        lock (mutex)
        {
            if (entries.TryGetValue(Key(database, sql), out Entry? entry))
                entry.State = EntryState.Prepared;
        }
    }

    /// <summary>
    /// Records that the server would not register the statement — it may not be preparable, or a
    /// server-side cap is full — so this client stops asking. The statement keeps running inline, which
    /// is what it was doing anyway.
    /// </summary>
    public void MarkRefused(string database, string sql)
    {
        lock (mutex)
        {
            if (entries.TryGetValue(Key(database, sql), out Entry? entry))
                entry.State = EntryState.Refused;
        }
    }

    /// <summary>
    /// Stops preparing anything, permanently: a node with no prepared-statement support answers every
    /// registration the same way, and spending one round trip per distinct statement to learn that again
    /// is worse than not trying.
    /// </summary>
    public void Disable() => disabled = true;

    /// <summary>Forgets a statement entirely, so a later execution counts from scratch.</summary>
    public void Forget(string database, string sql)
    {
        lock (mutex)
        {
            if (entries.Remove(Key(database, sql), out Entry? entry))
                recency.Remove(entry.Node!);
        }
    }

    private static string Key(string database, string sql) => database + "\n" + sql;

    /// <summary>
    /// Finds or creates the entry for a statement and moves it to the most-recent end, evicting the
    /// least recently used statement when a new entry would exceed the cap. Caller holds the lock.
    /// </summary>
    private Entry Touch(string database, string sql, ref (string Database, string Sql)? evicted)
    {
        string key = Key(database, sql);

        if (entries.TryGetValue(key, out Entry? entry))
        {
            recency.Remove(entry.Node!);
            recency.AddLast(entry.Node!);
            return entry;
        }

        if (entries.Count >= MaxAutoPrepare && recency.First is { } oldest)
        {
            recency.RemoveFirst();
            entries.Remove(Key(oldest.Value.Database, oldest.Value.Sql));

            // Only a registered statement is worth telling the caller about; the rest were never more
            // than a counter.
            if (oldest.Value.State == EntryState.Prepared)
                evicted = (oldest.Value.Database, oldest.Value.Sql);
        }

        entry = new Entry(database, sql);
        entry.Node = recency.AddLast(entry);
        entries[key] = entry;
        return entry;
    }

    private enum EntryState
    {
        /// <summary>Seen, but not yet often enough to be worth registering.</summary>
        Counting,

        /// <summary>A registration is in flight; the caller that started it will report the outcome.</summary>
        Registering,

        Prepared,

        /// <summary>The server would not register it. Runs inline from here on.</summary>
        Refused,
    }

    private sealed class Entry(string database, string sql)
    {
        public readonly string Database = database;

        public readonly string Sql = sql;

        public int Usages;

        public EntryState State = EntryState.Counting;

        /// <summary>This entry's place in the recency ordering.</summary>
        public LinkedListNode<Entry>? Node;
    }
}
