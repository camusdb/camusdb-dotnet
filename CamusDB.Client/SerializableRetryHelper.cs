
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// Helpers for serializable-isolation retry logic.
/// A failure is transient — resolvable by a full replay-from-BEGIN — when it carries one of the
/// retryable serialization-failure codes, or when its message contains one of the transient
/// contention markers the server emits without a distinct code (e.g. schema/sequence allocation
/// contention reported as "MustRetry"). All other failures are permanent and must propagate.
/// </summary>
public static class SerializableRetryHelper
{
    // CADB0502 — lock conflict; Kahuna aborted at lock-acquire time, no 2PC attempted.
    // CADB0504 — routing retry budget exhausted; no data written.
    // CADB0505 — transaction held open past MaxSerializableTransactionLifetimeMs; range locks released.
    // These three are "replay from BEGIN" transients: the transaction is dead and nothing it wrote
    // survived, so recovery re-runs the whole operation on a fresh transaction.
    private static readonly HashSet<string> RetryableCodes =
    [
        "CADB0502",
        "CADB0504",
        "CADB0505",
    ];

    /// <summary>
    /// CADB0509 <c>TransactionFinalizeUnresolved</c>: a <c>COMMIT</c>/<c>ROLLBACK</c> whose outcome is not
    /// yet known (the coordinator returned its non-terminal <c>MustRetry</c> past the bounded finalize
    /// retries). This is a fundamentally different kind of retry — the transaction is <b>not</b> dead and
    /// the finalize must be re-issued on the <b>same</b> handle (see
    /// <see cref="CamusTransaction.CommitAsync"/> / <see cref="CamusTransaction.RollbackAsync"/>). It must
    /// therefore <b>never</b> be treated as replay-from-<c>BEGIN</c> retryable — replaying a commit that
    /// may already have durably applied would double-apply it.
    /// </summary>
    internal const string FinalizeUnresolvedCode = "CADB0509";

    // Transient contention conditions the server surfaces in the message text rather than through a
    // distinct CADB05xx code. These fire when many operations race on shared schema/sequence state
    // (e.g. parallel environment provisioning colliding on the table-id sequence: "...: MustRetry").
    // Kept in sync with CamusConnection's CREATE DATABASE retry and BaseTest's DDL retry.
    private static readonly string[] RetryableMessageMarkers =
    [
        "MustRetry",
        "AlreadyLocked",
        "commit returned Aborted",
    ];

    /// <summary>
    /// Returns true when <paramref name="exception"/> is a transient serialization/contention failure —
    /// either by its <see cref="CamusException.Code"/> or by a transient marker in its message.
    /// </summary>
    public static bool IsRetryable(CamusException exception)
    {
        // CADB0509 is resolved by re-issuing the same finalize, never by a replay-from-BEGIN loop.
        // Exclude it up front so neither a code nor a message marker can classify it as replay-safe.
        if (exception.Code == FinalizeUnresolvedCode)
            return false;

        if (RetryableCodes.Contains(exception.Code))
            return true;

        string message = exception.Message;

        foreach (string marker in RetryableMessageMarkers)
        {
            if (message.Contains(marker, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Walks the exception chain and returns true if any <see cref="CamusException"/> in it is retryable.
    /// </summary>
    public static bool IsRetryable(Exception? exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is CamusException camusEx && IsRetryable(camusEx))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Executes <paramref name="operation"/> with bounded automatic retry on transient serialization
    /// failures (see <see cref="IsRetryable(CamusException)"/>). Any other exception propagates immediately.
    /// Back-off schedule: min(20 ms × 2^attempt, 400 ms) ± 25 % jitter.
    /// </summary>
    public static async Task ExecuteAutocommitAsync(
        Func<CancellationToken, Task> operation,
        int maxAttempts = 5,
        CancellationToken cancellationToken = default)
    {
        int attempt = 0;

        while (true)
        {
            try
            {
                await operation(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (CamusException ex) when (IsRetryable(ex))
            {
                if (++attempt >= maxAttempts)
                    throw;

                await Task.Delay(ComputeDelay(attempt), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Executes <paramref name="operation"/> with bounded automatic retry and returns its result.
    /// </summary>
    public static async Task<T> ExecuteAutocommitAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        int maxAttempts = 5,
        CancellationToken cancellationToken = default)
    {
        int attempt = 0;

        while (true)
        {
            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (CamusException ex) when (IsRetryable(ex))
            {
                if (++attempt >= maxAttempts)
                    throw;

                await Task.Delay(ComputeDelay(attempt), cancellationToken).ConfigureAwait(false);
            }
        }
    }

    // min(20 ms × 2^attempt, 400 ms) ± 25 % jitter
    private static TimeSpan ComputeDelay(int attempt)
    {
        double baseMs = Math.Min(20d * (1 << attempt), 400d);
        double jitter = baseMs * 0.25 * (2d * Random.Shared.NextDouble() - 1d);
        return TimeSpan.FromMilliseconds(Math.Max(1d, baseMs + jitter));
    }
}
