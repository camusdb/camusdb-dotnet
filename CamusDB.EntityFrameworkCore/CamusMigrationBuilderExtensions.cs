using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Migrations.Operations.Builders;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// View DDL for migrations.
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
    /// Quotes an identifier the way <see cref="CamusSqlGenerationHelper"/> does. A backtick inside the
    /// name is rejected rather than escaped: CamusDB's lexer trims the delimiters instead of decoding a
    /// doubled backtick, so there is no spelling that would survive, and emitting one anyway would fail
    /// midway through the migration as a parse error naming neither the view nor the column.
    /// </summary>
    private static string Delimit(string identifier, string parameterName)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("An identifier cannot be empty.", parameterName);

        if (identifier.Contains('`', StringComparison.Ordinal))
            throw new ArgumentException(
                $"The identifier '{identifier}' contains a backtick, which CamusDB cannot quote.", parameterName);

        return $"`{identifier}`";
    }
}
