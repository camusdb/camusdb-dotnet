/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// The foreign-key clause the provider composes as text, for migrations and <c>EnsureCreated</c>.
/// </summary>
/// <remarks>
/// <para>The server runs <c>NO ACTION</c> and <c>RESTRICT</c> only. It refuses <c>CASCADE</c>,
/// <c>SET NULL</c> and <c>SET DEFAULT</c> with <c>CADB0533</c>. EF Core gives a required relationship
/// <see cref="DeleteBehavior.Cascade"/> by default, so a refusal would fail the DDL of almost every
/// model. The provider renders those three actions as <c>NO ACTION</c> instead. EF Core still runs the
/// action on the dependents it tracks, as it does for <see cref="DeleteBehavior.ClientCascade"/> and
/// <see cref="DeleteBehavior.ClientSetNull"/>. A delete of a principal whose dependents are not tracked
/// fails with <c>CADB0305</c>.</para>
///
/// <para><c>NO ACTION</c> is the server default, so the clause for it is left out. The DDL then
/// matches what <c>SHOW CREATE TABLE</c> renders.</para>
/// </remarks>
internal static class CamusForeignKeySyntax
{
    /// <summary>
    /// Appends <c>CONSTRAINT name FOREIGN KEY (cols) REFERENCES table [(cols)] [ON DELETE RESTRICT]
    /// [ON UPDATE RESTRICT]</c>. An empty <paramref name="principalColumns"/> references the primary key
    /// of the principal table.
    /// </summary>
    internal static void AppendConstraint(
        StringBuilder sb,
        string name,
        IReadOnlyList<string> columns,
        string principalTable,
        IReadOnlyList<string>? principalColumns,
        ReferentialAction onDelete,
        ReferentialAction onUpdate)
    {
        sb.Append("CONSTRAINT ").Append(CamusIdentifier.Delimit(name, "foreignKeyName"))
          .Append(" FOREIGN KEY (").Append(DelimitList(columns, "foreignKeyColumn")).Append(')')
          .Append(" REFERENCES ").Append(CamusIdentifier.Delimit(principalTable, "principalTable"));

        if (principalColumns is { Count: > 0 })
            sb.Append(" (").Append(DelimitList(principalColumns, "principalColumn")).Append(')');

        if (onDelete == ReferentialAction.Restrict)
            sb.Append(" ON DELETE RESTRICT");

        if (onUpdate == ReferentialAction.Restrict)
            sb.Append(" ON UPDATE RESTRICT");
    }

    internal static string Constraint(
        string name,
        IReadOnlyList<string> columns,
        string principalTable,
        IReadOnlyList<string>? principalColumns,
        ReferentialAction onDelete,
        ReferentialAction onUpdate)
    {
        StringBuilder sb = new();
        AppendConstraint(sb, name, columns, principalTable, principalColumns, onDelete, onUpdate);
        return sb.ToString();
    }

    /// <summary>
    /// Whether the context's options let the provider emit foreign keys. A context without the CamusDB
    /// extension — none in practice — gets the default, <see langword="true"/>.
    /// </summary>
    internal static bool IsEnabled(IDbContextOptions options)
        => options.FindExtension<CamusDBOptionsExtension>()?.ForeignKeyConstraintsEnabled ?? true;

    internal static bool IsEnabled(DbContext context)
        => IsEnabled(context.GetService<IDbContextOptions>());

    private static string DelimitList(IReadOnlyList<string> names, string parameterName)
        => string.Join(", ", names.Select(n => CamusIdentifier.Delimit(n, parameterName)));
}
