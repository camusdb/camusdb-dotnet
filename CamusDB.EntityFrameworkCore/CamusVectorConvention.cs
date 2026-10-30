/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Adds <c>CHECK (vector_dims(column) = n)</c> to the table of each vector property that
/// <see cref="CamusPropertyBuilderExtensions.IsVector(PropertyBuilder, int, bool)"/> declares.
///
/// <para>The CHECK is added when the model is finalized, not when <c>IsVector</c> runs, because the SQL
/// names the column, and a <c>HasColumnName</c> or <c>ToTable</c> after <c>IsVector</c> changes that
/// name. A model snapshot already holds the CHECK as an ordinary check constraint, so the convention adds
/// nothing when a constraint of the same name exists.</para>
/// </summary>
public sealed class CamusVectorConvention : IModelFinalizingConvention
{
    public void ProcessModelFinalizing(IConventionModelBuilder modelBuilder, IConventionContext<IConventionModelBuilder> context)
    {
        foreach (IConventionEntityType entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            if (entityType.GetTableName() is not { } table)
                continue;

            StoreObjectIdentifier store = StoreObjectIdentifier.Table(table, entityType.GetSchema());

            foreach (IConventionProperty property in entityType.GetDeclaredProperties())
            {
                if (property.GetVectorDimensions() is not { } dimensions
                    || property.GetColumnName(store) is not { } column)
                    continue;

                string name = CheckConstraintName(table, column);
                if (entityType.FindCheckConstraint(name) is not null)
                    continue;

                entityType.AddCheckConstraint(name, CheckConstraintSql(column, dimensions));
            }
        }
    }

    /// <summary>The name of the dimension CHECK of a vector column.</summary>
    internal static string CheckConstraintName(string table, string column) => $"CK_{table}_{column}_dims";

    /// <summary><c>vector_dims(`column`) = dimensions</c>.</summary>
    internal static string CheckConstraintSql(string column, int dimensions)
        => $"vector_dims({CamusIdentifier.Delimit(column, nameof(column))}) = {dimensions.ToString(CultureInfo.InvariantCulture)}";
}
