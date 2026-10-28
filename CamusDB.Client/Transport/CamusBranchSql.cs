/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client.Transport;

/// <summary>
/// The SQL that both transports send for the database-branching operations, and the mapping of the rows
/// that the two SHOW statements return.
///
/// <para><b>Why SQL.</b> Branching exists on the server as SQL only: <c>CREATE DATABASE … BRANCH FROM …</c>,
/// <c>SHOW BRANCHES FROM …</c> and <c>SHOW ANCESTORS FROM …</c>. The server has no dedicated REST route for
/// them, so both transports send these statements through their ordinary DDL and query paths. This also
/// gives the statements the server's authorization checks and error codes.</para>
/// </summary>
internal static class CamusBranchSql
{
    // Database names are bare identifiers in the grammar; backtick-delimit so a name is never mistaken
    // for a keyword. A name carrying a backtick is refused before anything is sent — see
    // CamusSqlSyntax.DelimitIdentifier.
    public static string CreateBranch(string branchName, string sourceDatabaseName, bool ifNotExists)
    {
        string branch = CamusSqlSyntax.DelimitIdentifier(branchName, nameof(branchName));
        string source = CamusSqlSyntax.DelimitIdentifier(sourceDatabaseName, nameof(sourceDatabaseName));

        return ifNotExists
            ? $"CREATE DATABASE IF NOT EXISTS {branch} BRANCH FROM {source}"
            : $"CREATE DATABASE {branch} BRANCH FROM {source}";
    }

    public static string ShowBranches(string database)
        => $"SHOW BRANCHES FROM {CamusSqlSyntax.DelimitIdentifier(database, nameof(database))}";

    public static string ShowAncestors(string database)
        => $"SHOW ANCESTORS FROM {CamusSqlSyntax.DelimitIdentifier(database, nameof(database))}";

    // SHOW BRANCHES emits columns [database, id, depth, parent, fork_timestamp]; SHOW ANCESTORS emits
    // [database, id, depth, fork_timestamp] (no parent). Map by column name so either shape reconstructs.
    public static IReadOnlyList<CamusBranchRow> MapRows(CamusResultSet rows)
    {
        int columnCount = rows.ColumnCount;
        int databaseCol = -1, idCol = -1, depthCol = -1, parentCol = -1, forkCol = -1;

        for (int c = 0; c < columnCount; c++)
        {
            switch (rows.ColumnNames[c])
            {
                case "database": databaseCol = c; break;
                case "id": idCol = c; break;
                case "depth": depthCol = c; break;
                case "parent": parentCol = c; break;
                case "fork_timestamp": forkCol = c; break;
            }
        }

        List<CamusBranchRow> result = new(rows.RowCount);
        for (int r = 0; r < rows.RowCount; r++)
        {
            result.Add(new CamusBranchRow
            {
                Database = databaseCol >= 0 ? rows.GetCell(r, databaseCol).StrValue : null,
                Id = idCol >= 0 ? rows.GetCell(r, idCol).StrValue : null,
                Depth = depthCol >= 0 ? (int)rows.GetCell(r, depthCol).LongValue : 0,
                Parent = parentCol >= 0 ? rows.GetCell(r, parentCol).StrValue : null,
                ForkTimestamp = forkCol >= 0 ? rows.GetCell(r, forkCol).StrValue : null,
            });
        }

        return result;
    }
}
