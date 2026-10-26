/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Tests;

/// <summary>
/// The tables and the sanitized text the driver's spans take from a SQL text.
/// </summary>
public sealed class TestQueryText
{
    [Theory]
    [InlineData("SELECT * FROM robots", "robots")]
    [InlineData("select `r`.`id` from `robots` as `r` where `r`.`id` = @id", "robots")]
    [InlineData("SELECT * FROM shop.robots", "shop.robots")]
    [InlineData("SELECT * FROM `shop`.`robots` r", "shop.robots")]
    [InlineData("SELECT a.id FROM a INNER JOIN b ON b.a_id = a.id LEFT JOIN c ON c.id = b.c_id", "a,b,c")]
    [InlineData("SELECT * FROM a JOIN a AS a2 ON a2.parent = a.id", "a")]
    [InlineData("INSERT INTO `wallets` (`id`, `amount`) VALUES (@p0, @p1)", "wallets")]
    [InlineData("INSERT INTO audit SELECT * FROM events", "audit,events")]
    [InlineData("UPDATE `players` SET `name` = @p0 WHERE `id` = @p1", "players")]
    [InlineData("DELETE FROM sessions WHERE expires < 100", "sessions")]
    [InlineData("INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET id = 2", "t")]
    [InlineData("CREATE TABLE IF NOT EXISTS robots (id OID PRIMARY KEY)", "robots")]
    [InlineData("DROP TABLE IF EXISTS robots", "robots")]
    [InlineData("TRUNCATE robots", "robots")]
    [InlineData("TRUNCATE TABLE robots", "robots")]
    [InlineData("SELECT * FROM (SELECT id FROM inner_table) AS x", "inner_table")]
    [InlineData("SELECT * FROM a WHERE id IN (SELECT a_id FROM b)", "a,b")]
    [InlineData("SELECT EXTRACT(YEAR FROM created) FROM orders", "orders")]
    [InlineData("SELECT TRIM(LEADING 'x' FROM name) FROM robots", "robots")]
    [InlineData("SELECT * FROM -- comment\n robots", "robots")]
    [InlineData("SELECT 'FROM fake' FROM robots", "robots")]
    [InlineData("SELECT 1", "")]
    [InlineData("BEGIN", "")]
    public void Tables_NamesEachTableOnceInOrder(string sql, string expected)
    {
        string[] tables = expected.Length == 0 ? [] : expected.Split(',');

        Assert.Equal(tables, CamusQueryText.Tables(sql));
    }

    [Fact]
    public void Summary_IsTheOperationAndItsTables()
    {
        Assert.Equal("SELECT orders customers", CamusQueryText.Summary("SELECT", ["orders", "customers"]));
        Assert.Equal("orders", CamusQueryText.Summary(null, ["orders"]));
        Assert.Null(CamusQueryText.Summary("SELECT", []));
    }

    [Fact]
    public void Summary_StopsAtTheLastWholeTableThatFits()
    {
        string[] tables = Enumerable.Range(0, 40).Select(index => $"table_number_{index:D2}").ToArray();

        string summary = CamusQueryText.Summary("SELECT", tables)!;

        Assert.True(summary.Length <= CamusQueryText.MaxSummaryLength);
        Assert.StartsWith("SELECT table_number_00 table_number_01", summary);
        Assert.EndsWith(tables.Last(name => summary.Contains(name, StringComparison.Ordinal)), summary);
    }

    [Fact]
    public void AnyText_IsReadWithoutAnException()
    {
        // The scan runs inside every sampled command, so it must accept any text, finished or not.
        const string alphabet = "SELECT FROM JOIN INTO UPDATE TABLE IF NOT EXISTS TRUNCATE EXTRACT(`'\"\\-/*@.,;()0123456789xXeE \n";
        Random random = new(20261026);
        for (int sample = 0; sample < 20_000; sample++)
        {
            char[] text = new char[random.Next(0, 40)];
            for (int index = 0; index < text.Length; index++)
                text[index] = alphabet[random.Next(alphabet.Length)];

            string sql = new(text);
            List<string> tables = CamusQueryText.Tables(sql);
            CamusQueryText.Summary("SELECT", tables);
            CamusQueryText.Sanitize(sql);
        }
    }

    [Theory]
    [InlineData("SELECT * FROM t WHERE email = 'a@b.co'", "SELECT * FROM t WHERE email = ?")]
    [InlineData("SELECT * FROM t WHERE name = \"Ana\"", "SELECT * FROM t WHERE name = ?")]
    [InlineData("SELECT * FROM t WHERE name = 'O''Brien'", "SELECT * FROM t WHERE name = ?")]
    [InlineData("SELECT * FROM t WHERE path = 'C:\\' AND id = 1", "SELECT * FROM t WHERE path = ? AND id = ?")]
    [InlineData("SELECT * FROM t WHERE note = E'it\\'s' AND id = 2", "SELECT * FROM t WHERE note = ? AND id = ?")]
    [InlineData("SELECT * FROM t WHERE data = x'DEADBEEF'", "SELECT * FROM t WHERE data = ?")]
    [InlineData("SELECT * FROM t WHERE score > -1.5e3 AND flag = 0x1F", "SELECT * FROM t WHERE score > -? AND flag = ?")]
    [InlineData("SELECT * FROM t2 WHERE `t2`.`col1` = @p0", "SELECT * FROM t2 WHERE `t2`.`col1` = @p0")]
    [InlineData("SELECT * FROM t WHERE id IN ('a', 'b', 'c', 'd')", "SELECT * FROM t WHERE id IN (?, ...)")]
    [InlineData("SELECT * FROM t WHERE id = 1 -- player 42\nLIMIT 5", "SELECT * FROM t WHERE id = ?  \nLIMIT ?")]
    [InlineData("SELECT /* user 7 */ id FROM t", "SELECT   id FROM t")]
    [InlineData("SELECT * FROM t WHERE b = TRUE AND c IS NULL", "SELECT * FROM t WHERE b = TRUE AND c IS NULL")]
    [InlineData("SELECT * FROM t WHERE s = 'unterminated", "SELECT * FROM t WHERE s = ?")]
    public void Sanitize_ReplacesEachLiteralValue(string sql, string expected)
    {
        Assert.Equal(expected, CamusQueryText.Sanitize(sql));
    }
}
