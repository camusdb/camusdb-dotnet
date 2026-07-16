/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Tests;

public class TestSerializableRetryHelper
{
    [Theory]
    [InlineData("CADB0502")]
    [InlineData("CADB0504")]
    [InlineData("CADB0505")]
    public void RetryableCodesAreRetryable(string code)
    {
        CamusException ex = new(code, "serialization failure");

        Assert.True(SerializableRetryHelper.IsRetryable(ex));
        Assert.True(SerializableRetryHelper.IsRetryable((Exception)ex));
    }

    [Theory]
    [InlineData("Failed to ensure table id sequence: MustRetry")]
    [InlineData("Lock AlreadyLocked on range")]
    [InlineData("Transaction commit returned Aborted")]
    public void TransientMessageMarkersAreRetryable(string message)
    {
        // Server surfaces these transient contention conditions in the message rather than via a
        // distinct CADB05xx code (e.g. the migration DDL "MustRetry" seen under parallel provisioning).
        CamusException ex = new("CADB0000", message);

        Assert.True(SerializableRetryHelper.IsRetryable(ex));
        Assert.True(SerializableRetryHelper.IsRetryable((Exception)ex));
    }

    [Fact]
    public void PermanentErrorsAreNotRetryable()
    {
        CamusException ex = new("CADB0100", "Table 'people' does not exist");

        Assert.False(SerializableRetryHelper.IsRetryable(ex));
        Assert.False(SerializableRetryHelper.IsRetryable((Exception)ex));
    }

    [Fact]
    public void MarkerMatchIsCaseSensitiveAndOrdinal()
    {
        // The markers mirror the exact server wording; a lower-cased variant is not a match.
        CamusException ex = new("CADB0100", "mustretry later");

        Assert.False(SerializableRetryHelper.IsRetryable(ex));
    }

    [Fact]
    public void RetryableExceptionNestedInChainIsDetected()
    {
        CamusException inner = new("CADB0000", "Failed to ensure table id sequence: MustRetry");
        InvalidOperationException outer = new("wrapped", inner);

        Assert.True(SerializableRetryHelper.IsRetryable(outer));
    }

    [Fact]
    public void NonCamusExceptionIsNotRetryable()
    {
        Assert.False(SerializableRetryHelper.IsRetryable(new InvalidOperationException("MustRetry")));
        Assert.False(SerializableRetryHelper.IsRetryable((Exception?)null));
    }

    [Fact]
    public async Task ExecuteAutocommitRetriesTransientMessageThenSucceeds()
    {
        int attempts = 0;

        await SerializableRetryHelper.ExecuteAutocommitAsync(_ =>
        {
            attempts++;
            if (attempts < 3)
                throw new CamusException("CADB0000", "Failed to ensure table id sequence: MustRetry");

            return Task.CompletedTask;
        });

        Assert.Equal(3, attempts);
    }

    [Fact]
    public async Task ExecuteAutocommitDoesNotRetryPermanentError()
    {
        int attempts = 0;

        await Assert.ThrowsAsync<CamusException>(() =>
            SerializableRetryHelper.ExecuteAutocommitAsync(_ =>
            {
                attempts++;
                throw new CamusException("CADB0100", "Table does not exist");
            }));

        Assert.Equal(1, attempts);
    }
}
