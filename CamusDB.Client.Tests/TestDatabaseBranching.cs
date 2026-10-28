/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client.Transport;

namespace CamusDB.Client.Tests;

/// <summary>
/// The SQL both transports send for the branch operations. The server has these operations as SQL only,
/// so the REST transport must send the same statements as the gRPC transport (until 0.16.7 it called
/// REST routes that never existed, and every call failed with an HTTP 404).
/// </summary>
public class TestDatabaseBranching
{
    [Fact]
    public void CreateBranchIsDelimitedSql()
    {
        Assert.Equal("CREATE DATABASE `b` BRANCH FROM `a`", CamusBranchSql.CreateBranch("b", "a", ifNotExists: false));
        Assert.Equal("CREATE DATABASE IF NOT EXISTS `b` BRANCH FROM `a`", CamusBranchSql.CreateBranch("b", "a", ifNotExists: true));
    }

    [Fact]
    public void ShowStatementsAreDelimitedSql()
    {
        Assert.Equal("SHOW BRANCHES FROM `select`", CamusBranchSql.ShowBranches("select"));
        Assert.Equal("SHOW ANCESTORS FROM `select`", CamusBranchSql.ShowAncestors("select"));
    }

    [Fact]
    public void ABacktickInANameIsRefused()
    {
        Assert.Equal("branchName", Assert.Throws<ArgumentException>(() => CamusBranchSql.CreateBranch("b`x", "a", false)).ParamName);
        Assert.Equal("sourceDatabaseName", Assert.Throws<ArgumentException>(() => CamusBranchSql.CreateBranch("b", "a`x", false)).ParamName);
        Assert.Throws<ArgumentException>(() => CamusBranchSql.ShowBranches("a`x"));
        Assert.Throws<ArgumentException>(() => CamusBranchSql.ShowAncestors(""));
    }

    // The endpoint refuses every connection. An ArgumentException, not a CamusException, shows that the
    // name was refused before the transport sent anything.
    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ABacktickIsRefusedBeforeAnythingIsSent(string protocol)
    {
        await using CamusConnection connection = new(
            new CamusConnectionStringBuilder($"Endpoint=http://127.0.0.1:1;Database=test;Protocol={protocol}"));

        await Assert.ThrowsAsync<ArgumentException>(() => connection.CreateBranchDatabaseAsync("b`x", "a"));
        await Assert.ThrowsAsync<ArgumentException>(() => connection.CreateBranchDatabaseAsync("b", "a`x"));
        await Assert.ThrowsAsync<ArgumentException>(() => connection.ShowBranchesAsync("a`x"));
        await Assert.ThrowsAsync<ArgumentException>(() => connection.ShowAncestorsAsync("a`x"));
    }
}
