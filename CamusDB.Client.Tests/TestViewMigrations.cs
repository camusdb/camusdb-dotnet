/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CamusDB.Client.Tests;

/// <summary>
/// The SQL <see cref="CamusMigrationBuilderExtensions"/> puts into a migration. These assert the text
/// because the text is the contract: it is handed to the server verbatim.
/// </summary>
public class TestViewMigrations
{
    private static string SqlOf(MigrationBuilder builder)
        => Assert.IsType<SqlOperation>(Assert.Single(builder.Operations)).Sql;

    private static MigrationBuilder NewBuilder() => new("CamusDB");

    [Fact]
    public void TestCreateView()
    {
        MigrationBuilder builder = NewBuilder();
        builder.CreateView("open_orders", "SELECT id, customer FROM orders WHERE status = 'open'");

        Assert.Equal(
            "CREATE VIEW `open_orders` AS SELECT id, customer FROM orders WHERE status = 'open'",
            SqlOf(builder));
    }

    [Fact]
    public void TestCreateViewWithColumnNames()
    {
        MigrationBuilder builder = NewBuilder();
        builder.CreateView("order_summary", "SELECT id, customer FROM orders", columns: ["order_id", "who"]);

        Assert.Equal(
            "CREATE VIEW `order_summary` (`order_id`, `who`) AS SELECT id, customer FROM orders",
            SqlOf(builder));
    }

    [Fact]
    public void TestCreateOrReplaceView()
    {
        MigrationBuilder builder = NewBuilder();
        builder.CreateView("v", "SELECT id FROM orders", orReplace: true);

        Assert.Equal("CREATE OR REPLACE VIEW `v` AS SELECT id FROM orders", SqlOf(builder));
    }

    [Fact]
    public void TestDropView()
    {
        MigrationBuilder builder = NewBuilder();
        builder.DropView("open_orders");

        Assert.Equal("DROP VIEW `open_orders`", SqlOf(builder));
    }

    [Fact]
    public void TestDropViewIfExistsCascade()
    {
        MigrationBuilder builder = NewBuilder();
        builder.DropView("open_orders", ifExists: true, cascade: true);

        Assert.Equal("DROP VIEW IF EXISTS `open_orders` CASCADE", SqlOf(builder));
    }

    [Fact]
    public void TestRenameView()
    {
        MigrationBuilder builder = NewBuilder();
        builder.RenameView("open_orders", "active_orders");

        Assert.Equal("ALTER VIEW `open_orders` RENAME TO `active_orders`", SqlOf(builder));
    }

    [Fact]
    public void TestEmptyBodyIsRejected()
    {
        MigrationBuilder builder = NewBuilder();

        Assert.Throws<ArgumentException>(() => builder.CreateView("v", "   "));
    }

    /// <summary>
    /// CamusDB's lexer trims the delimiters rather than decoding a doubled backtick, so a backtick in
    /// a name has no spelling that survives. It is caught here instead of failing as a parse error
    /// partway through the migration.
    /// </summary>
    [Fact]
    public void TestBacktickInIdentifierIsRejected()
    {
        MigrationBuilder builder = NewBuilder();

        Assert.Throws<ArgumentException>(() => builder.CreateView("we`ird", "SELECT id FROM orders"));
        Assert.Throws<ArgumentException>(
            () => builder.CreateView("v", "SELECT id FROM orders", columns: ["we`ird"]));
        Assert.Throws<ArgumentException>(() => builder.DropView("we`ird"));
        Assert.Throws<ArgumentException>(() => builder.RenameView("v", "we`ird"));
    }
}
