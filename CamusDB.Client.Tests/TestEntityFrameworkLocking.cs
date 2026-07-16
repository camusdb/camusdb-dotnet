/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.Client.Tests;

/// <summary>
/// Deterministic (server-free) coverage of the EF provider's concurrency-defaults wiring:
/// <c>UseOptimisticLocking()</c> / <c>UseTransactionDefaults()</c> store options on the provider
/// extension so the created <see cref="CamusConnection"/> inherits them.
/// </summary>
public class TestEntityFrameworkLocking
{
    private const string ConnectionString = "Endpoint=http://localhost:5095;Database=test";

    private static CamusDBOptionsExtension? Extension(Action<CamusDBDbContextOptionsBuilder> configure)
    {
        DbContextOptions options = new DbContextOptionsBuilder()
            .UseCamusDB(ConnectionString, configure)
            .Options;

        return options.FindExtension<CamusDBOptionsExtension>();
    }

    [Fact]
    public void UseOptimisticLockingStoresOptimisticDefault()
    {
        CamusDBOptionsExtension? extension = Extension(c => c.UseOptimisticLocking());

        Assert.NotNull(extension);
        Assert.Equal(CamusLocking.Optimistic, extension!.DefaultTransactionOptions?.Locking);
    }

    [Fact]
    public void UsePessimisticLockingStoresPessimisticDefault()
    {
        CamusDBOptionsExtension? extension = Extension(c => c.UsePessimisticLocking());

        Assert.Equal(CamusLocking.Pessimistic, extension!.DefaultTransactionOptions?.Locking);
    }

    [Fact]
    public void UseTransactionDefaultsStoresAllKnobs()
    {
        CamusDBOptionsExtension? extension = Extension(c => c.UseTransactionDefaults(new CamusTransactionOptions
        {
            IsolationLevel = CamusIsolationLevel.ReadCommitted,
            Mode = CamusTransactionMode.ReadOnly,
            Locking = CamusLocking.Optimistic,
        }));

        CamusTransactionOptions? defaults = extension!.DefaultTransactionOptions;
        Assert.Equal(CamusIsolationLevel.ReadCommitted, defaults?.IsolationLevel);
        Assert.Equal(CamusTransactionMode.ReadOnly, defaults?.Mode);
        Assert.Equal(CamusLocking.Optimistic, defaults?.Locking);
    }

    [Fact]
    public void NoLockingConfigurationLeavesDefaultsUnset()
    {
        CamusDBOptionsExtension? extension = Extension(_ => { });

        Assert.Null(extension!.DefaultTransactionOptions);
    }

    [Fact]
    public void LockingDefaultSurvivesRetryConfiguration()
    {
        // Options are immutable clones threaded through AddOrUpdateExtension; a later EnableRetryOnFailure
        // must not drop an earlier UseOptimisticLocking (and vice-versa).
        CamusDBOptionsExtension? extension = Extension(c => c.UseOptimisticLocking().EnableRetryOnFailure());

        Assert.Equal(CamusLocking.Optimistic, extension!.DefaultTransactionOptions?.Locking);
        Assert.True(extension.RetryOnFailureEnabled);
    }
}
