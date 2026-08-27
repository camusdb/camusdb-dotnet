using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations.Operations.Builders;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// View DDL, and <c>TRUNCATE TABLE</c>, for migrations.
/// </summary>
/// <remarks>
/// <para>EF Core has no migration operation for views, so a provider that wants them has to emit raw
/// SQL. These wrap <see cref="MigrationBuilder.Sql"/> so a migration says what it means and gets the
/// identifier quoting right, rather than hand-concatenating a statement per migration.</para>
///
/// <para>A view is mapped with <c>ToView("name")</c> on the entity type, which EF excludes from
/// migrations — nothing generates these calls for you; write them in the migration's <c>Up</c> and
/// the matching drop in its <c>Down</c>.</para>
/// </remarks>
public static class CamusMigrationBuilderExtensions
{
    /// <summary>
    /// Emits <c>CREATE VIEW</c> over <paramref name="selectSql"/>.
    /// </summary>
    /// <param name="name">The view's name.</param>
    /// <param name="selectSql">
    /// The body, a single SELECT. Every output column must have a name — CamusDB refuses a bare
    /// expression rather than inventing one, so alias computed projections (<c>total + 1 AS total_plus_one</c>).
    /// The body may not mix <c>*</c> with other projections, and may carry neither
    /// <c>AS OF SYSTEM TIME</c> nor index/cache hints.
    /// </param>
    /// <param name="columns">
    /// Optional names for the view's own columns, replacing the body's. Must match the body's column
    /// count.
    /// </param>
    /// <param name="orReplace">
    /// Emit <c>CREATE OR REPLACE VIEW</c>. CamusDB allows a replacement to <em>append</em> columns
    /// only: existing names, types, and order must be preserved, because dependent views and cached
    /// plans are already bound to them. To change the shape, drop and recreate — which is a separate
    /// migration step, and forces the dependents into the open.
    /// </param>
    public static OperationBuilder<SqlOperation> CreateView(
        this MigrationBuilder migrationBuilder,
        string name,
        string selectSql,
        IEnumerable<string>? columns = null,
        bool orReplace = false)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);

        if (string.IsNullOrWhiteSpace(selectSql))
            throw new ArgumentException("A view needs a body; 'selectSql' is empty.", nameof(selectSql));

        string columnList = columns is null
            ? ""
            : $" ({string.Join(", ", columns.Select(c => Delimit(c, nameof(columns))))})";

        return migrationBuilder.Sql(
            $"CREATE {(orReplace ? "OR REPLACE " : "")}VIEW {Delimit(name, nameof(name))}{columnList} AS {selectSql}");
    }

    /// <summary>
    /// Emits <c>DROP VIEW</c>.
    /// </summary>
    /// <param name="ifExists">Do not fail when the view is already gone.</param>
    /// <param name="cascade">
    /// Drop the views that depend on this one too. Without it, CamusDB refuses a drop that would
    /// orphan a dependent (CADB0530) — so leave this off unless the migration means to take the
    /// dependents with it, and prefer dropping them in their own explicit steps.
    /// </param>
    public static OperationBuilder<SqlOperation> DropView(
        this MigrationBuilder migrationBuilder,
        string name,
        bool ifExists = false,
        bool cascade = false)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);

        return migrationBuilder.Sql(
            $"DROP VIEW {(ifExists ? "IF EXISTS " : "")}{Delimit(name, nameof(name))}{(cascade ? " CASCADE" : "")}");
    }

    /// <summary>
    /// Emits <c>ALTER VIEW … RENAME TO</c>. Metadata-only: the view keeps its id, so dependents keep
    /// resolving.
    /// </summary>
    public static OperationBuilder<SqlOperation> RenameView(
        this MigrationBuilder migrationBuilder,
        string name,
        string newName)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);

        return migrationBuilder.Sql(
            $"ALTER VIEW {Delimit(name, nameof(name))} RENAME TO {Delimit(newName, nameof(newName))}");
    }

    /// <summary>
    /// Emits <c>TRUNCATE TABLE</c>, which empties a base table without a read or a delete of a single
    /// row. The table keeps its name, its id, its columns, its indexes and its grants; only its contents
    /// generation moves. The cost is the same on an empty table and on a billion-row one.
    /// </summary>
    /// <remarks>
    /// <para>The command suppresses the migration transaction, and it has to. <c>TRUNCATE</c> commits a
    /// replicated schema entry that a <c>ROLLBACK</c> cannot undo, so CamusDB refuses the statement
    /// inside an explicit transaction (<c>CADB0538</c>) — which is the transaction EF Core wraps a
    /// migration in. A failure after this step therefore leaves the table emptied.</para>
    ///
    /// <para>The previous contents are retained as recoverable retired contents for the server's
    /// retention window: <c>SHOW ORPHAN TABLES</c> lists them, and
    /// <c>CREATE TABLE t_before RELINK TO '&lt;id&gt;'</c> brings them back as a separate table. Write the
    /// migration's <c>Down</c> with that in mind — a truncate has no automatic inverse.</para>
    ///
    /// <para>The optimizer's statistics for the new contents start out unmeasured, so run <c>ANALYZE</c>
    /// once the table is repopulated.</para>
    /// </remarks>
    public static OperationBuilder<SqlOperation> TruncateTable(
        this MigrationBuilder migrationBuilder,
        string name)
    {
        ArgumentNullException.ThrowIfNull(migrationBuilder);

        return migrationBuilder.Sql(
            $"TRUNCATE TABLE {Delimit(name, nameof(name))}",
            suppressTransaction: true);
    }

    private static string Delimit(string identifier, string parameterName)
        => CamusIdentifier.Delimit(identifier, parameterName);
}
