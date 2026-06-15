
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// Helpers for serializable-isolation retry logic.
/// Only three error codes indicate a transient serialization failure that a full
/// replay-from-BEGIN can resolve; all others are permanent and must propagate.
/// </summary>
public static class SerializableRetryHelper
{
    // CADB0502 — lock conflict; Kahuna aborted at lock-acquire time, no 2PC attempted.
    // CADB0504 — routing retry budget exhausted; no data written.
    // CADB0505 — transaction held open past MaxSerializableTransactionLifetimeMs; range locks released.
    private static readonly HashSet<string> RetryableCodes =
    [
        "CADB0502",
        "CADB0504",
        "CADB0505",
    ];

    /// <summary>Returns true when <paramref name="exception"/> carries a retryable serialization-failure code.</summary>
    public static bool IsRetryable(CamusException exception)
        => RetryableCodes.Contains(exception.Code);

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
    /// failures (CADB0502 / CADB0504 / CADB0505). Any other exception propagates immediately.
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
