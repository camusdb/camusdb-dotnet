/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// CamusDB-specific property configuration.
/// </summary>
public static class CamusPropertyBuilderExtensions
{
    /// <summary>
    /// Sets how the server may store a large value of this column: compressed, out of the row, both or
    /// neither. Migrations emit it as an inline <c>STORAGE</c> clause on <c>CREATE TABLE</c> and
    /// <c>ADD COLUMN</c>, and a later change as <c>ALTER COLUMN … SET STORAGE</c>. Pass
    /// <see langword="null"/> to fall back to the server default, <see cref="CamusColumnStorage.Extended"/>.
    /// </summary>
    /// <remarks>
    /// <para>Only a <c>string</c>, <c>bytes</c> or array column has a storage strategy. The model
    /// validator refuses one on any other column type, which the server refuses with <c>CADB0414</c>.</para>
    ///
    /// <para>A change of strategy applies to future writes only. It does not rewrite the rows that
    /// already exist; add <see cref="CamusMigrationBuilderExtensions.RewriteStorage"/> to the migration
    /// to convert them.</para>
    ///
    /// <para>An embedding column that a KNN query scans should use <see cref="CamusColumnStorage.Plain"/>.
    /// A 768-dimension embedding is 3072 bytes, which is above the default out-of-line threshold, so under
    /// the default strategy every embedding moves out of its row and every scanned batch costs one more
    /// fetch.</para>
    /// </remarks>
    public static PropertyBuilder HasStorage(this PropertyBuilder propertyBuilder, CamusColumnStorage? storage)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetStorage(storage);
        return propertyBuilder;
    }

    /// <inheritdoc cref="HasStorage(PropertyBuilder, CamusColumnStorage?)" />
    public static PropertyBuilder<TProperty> HasStorage<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        CamusColumnStorage? storage)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetStorage(storage);
        return propertyBuilder;
    }

    /// <summary>Returns the storage strategy configured on the property, or <see langword="null"/> for the server default.</summary>
    public static CamusColumnStorage? GetStorage(this IReadOnlyProperty property)
        => (CamusColumnStorage?)property[CamusAnnotationNames.ColumnStorage];

    /// <summary>Sets the storage strategy of the property. <see langword="null"/> removes it.</summary>
    public static void SetStorage(this IMutableProperty property, CamusColumnStorage? storage)
    {
        if (storage is { } value && !Enum.IsDefined(value))
            throw new ArgumentOutOfRangeException(nameof(storage), value, "Unknown CamusDB column storage strategy.");

        property.SetOrRemoveAnnotation(CamusAnnotationNames.ColumnStorage, storage);
    }
}
