/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Tests;

/// <summary>
/// Server-backed coverage of <c>CREATE DATABASE ... IF NOT EXISTS</c> idempotency, including the
/// concurrent-registration race: two racing creates of the same name can both pass the server's
/// existence check and the loser is rejected with <c>DatabaseAlreadyExists</c> (CADB0012). With
/// <c>ifNotExists: true</c> the client treats that as success, so a race never surfaces to the caller.
/// </summary>
public class TestCreateDatabaseIdempotent : BaseTest
{
    private static string UniqueDb() => "cdb9_" + Guid.NewGuid().ToString("n");

    [Fact]
    public async Task CreateIfNotExistsTwiceDoesNotThrow()
    {
        CamusConnection connection = await GetConnection();
        string name = UniqueDb();

        try
        {
            await connection.CreateDatabaseAsync(name, ifNotExists: true);
            await connection.CreateDatabaseAsync(name, ifNotExists: true);
        }
        finally
        {
            await connection.DropDatabaseAsync(name);
        }
    }

    [Fact]
    public async Task CreateWithoutIfNotExistsOnExistingThrowsAlreadyExists()
    {
        CamusConnection connection = await GetConnection();
        string name = UniqueDb();

        await connection.CreateDatabaseAsync(name, ifNotExists: true);
        try
        {
            CamusException ex = await Assert.ThrowsAsync<CamusException>(
                () => connection.CreateDatabaseAsync(name, ifNotExists: false));

            Assert.Equal("CADB0012", ex.Code);
        }
        finally
        {
            await connection.DropDatabaseAsync(name);
        }
    }

    [Fact]
    public async Task ConcurrentCreateIfNotExistsForSameNameAllSucceed()
    {
        CamusConnection connection = await GetConnection();
        string name = UniqueDb();

        try
        {
            // Fire many creates of the same fresh name at once. Some pass the server's existence check
            // together and collide at registration; ifNotExists tolerance must absorb the CADB0012 loser
            // so none of these throw. (Without the tolerance this fails whenever the race triggers.)
            Task[] creates = [.. Enumerable.Range(0, 12).Select(_ =>
                connection.CreateDatabaseAsync(name, ifNotExists: true))];

            await Task.WhenAll(creates);
        }
        finally
        {
            await connection.DropDatabaseAsync(name);
        }
    }
}
