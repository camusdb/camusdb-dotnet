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
using Microsoft.EntityFrameworkCore.ValueGeneration;

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

    /// <summary>The block size <see cref="UseHiLo(PropertyBuilder, string, int?)"/> gives a sequence it creates.</summary>
    public const int DefaultHiLoBlockSize = 10;

    /// <summary>
    /// Gives the property its value from the sequence <paramref name="sequenceName"/>, one
    /// <c>SELECT nextval('…')</c> for each new entity. The property must be an integer (<c>long</c>,
    /// <c>int</c> or <c>short</c>).
    /// </summary>
    /// <remarks>
    /// <para>This call does four things:</para>
    /// <list type="number">
    /// <item>It adds the sequence to the model if it is not there yet. Configure it with
    /// <c>modelBuilder.HasSequence(name)</c> to set a start value, a minimum or a maximum.</item>
    /// <item>It marks the property as generated on add.</item>
    /// <item>It sets the column default to <c>nextval('…')</c>, so an <c>INSERT</c> in SQL that omits
    /// the column also draws from the sequence.</item>
    /// <item>It installs <see cref="CamusSequenceValueGenerator{TValue}"/>, which draws the value when the
    /// entity is added to the context.</item>
    /// </list>
    ///
    /// <para>The value is drawn on the client because CamusDB has no <c>RETURNING</c> clause: EF cannot
    /// read back a value that the server generated. An entity that has a value other than the CLR
    /// default keeps it, and the sequence does not advance for it.</para>
    ///
    /// <para>A sequence value is unique, but it is not an insert order. A rolled-back transaction does
    /// not return the values it drew. See <c>docs/sequences.md</c> in the server repository.</para>
    ///
    /// <para>Each value costs one round trip and, with the server default <c>CACHE 1</c>, one durable
    /// commit. For bulk inserts, use <see cref="UseHiLo(PropertyBuilder, string, int?)"/>.</para>
    /// </remarks>
    public static PropertyBuilder UseSequence(this PropertyBuilder propertyBuilder, string sequenceName)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetSequence(sequenceName, hiLo: false, blockSize: null);
        return propertyBuilder;
    }

    /// <inheritdoc cref="UseSequence(PropertyBuilder, string)" />
    public static PropertyBuilder<TProperty> UseSequence<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        string sequenceName)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetSequence(sequenceName, hiLo: false, blockSize: null);
        return propertyBuilder;
    }

    /// <summary>
    /// Gives the property its values from the sequence <paramref name="sequenceName"/> in blocks. One
    /// <c>nextval</c> reserves a block of values for this process, so most new entities need no round
    /// trip. The property must be an integer (<c>long</c>, <c>int</c> or <c>short</c>).
    /// </summary>
    /// <param name="propertyBuilder">The property.</param>
    /// <param name="sequenceName">The sequence to draw from.</param>
    /// <param name="blockSize">
    /// The number of values in one block, which is also the <c>INCREMENT BY</c> of the sequence. When
    /// <see langword="null"/>, the increment of the sequence stays as configured, or is
    /// <see cref="DefaultHiLoBlockSize"/> for a sequence that this call adds to the model.
    /// </param>
    /// <remarks>
    /// <para>The configuration is the same as for <see cref="UseSequence(PropertyBuilder, string)"/>,
    /// with one difference: a value <c>v</c> from <c>nextval</c> reserves <c>v … v + blockSize − 1</c>.
    /// The blocks of two processes never overlap, because each block starts at a value that the
    /// sequence issued once only. An <c>INSERT</c> in SQL that uses the column default takes one value
    /// of the lattice and leaves the rest of that block unused.</para>
    ///
    /// <para>The values of a block that a process does not use are lost when the process stops. This
    /// makes gaps. Also, two processes use their blocks at the same time, so the values do not follow
    /// the insert order.</para>
    ///
    /// <para>Do not move the counter with <c>setval</c> or <c>RESTART</c> to a value that is not
    /// <c>start + k × blockSize</c>. A block that starts off the lattice can overlap values that another
    /// block already issued.</para>
    /// </remarks>
    public static PropertyBuilder UseHiLo(this PropertyBuilder propertyBuilder, string sequenceName, int? blockSize = null)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetSequence(sequenceName, hiLo: true, blockSize);
        return propertyBuilder;
    }

    /// <inheritdoc cref="UseHiLo(PropertyBuilder, string, int?)" />
    public static PropertyBuilder<TProperty> UseHiLo<TProperty>(
        this PropertyBuilder<TProperty> propertyBuilder,
        string sequenceName,
        int? blockSize = null)
    {
        ArgumentNullException.ThrowIfNull(propertyBuilder);

        propertyBuilder.Metadata.SetSequence(sequenceName, hiLo: true, blockSize);
        return propertyBuilder;
    }

    /// <summary>
    /// Returns the sequence configured with <see cref="UseSequence(PropertyBuilder, string)"/>, or
    /// <see langword="null"/>.
    /// </summary>
    public static string? GetSequenceName(this IReadOnlyProperty property)
        => (string?)property[CamusAnnotationNames.SequenceName];

    /// <summary>
    /// Returns the sequence configured with <see cref="UseHiLo(PropertyBuilder, string, int?)"/>, or
    /// <see langword="null"/>.
    /// </summary>
    public static string? GetHiLoSequenceName(this IReadOnlyProperty property)
        => (string?)property[CamusAnnotationNames.HiLoSequenceName];

    private static void SetSequence(this IMutableProperty property, string sequenceName, bool hiLo, int? blockSize)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sequenceName);
        CamusSqlSyntax.ValidateIdentifier(sequenceName, nameof(sequenceName));

        if (blockSize is < 1)
            throw new ArgumentOutOfRangeException(nameof(blockSize), blockSize, "A HiLo block size must be at least 1.");

        IMutableModel model = property.DeclaringType.Model;
        IMutableSequence? sequence = model.FindSequence(sequenceName);

        if (sequence is null)
        {
            sequence = model.AddSequence(sequenceName);
            if (hiLo)
                sequence.IncrementBy = blockSize ?? DefaultHiLoBlockSize;
        }
        else if (hiLo && blockSize is { } size)
        {
            sequence.IncrementBy = size;
        }

        property.SetOrRemoveAnnotation(CamusAnnotationNames.SequenceName, hiLo ? null : sequenceName);
        property.SetOrRemoveAnnotation(CamusAnnotationNames.HiLoSequenceName, hiLo ? sequenceName : null);
        property.ValueGenerated = ValueGenerated.OnAdd;
        property.SetDefaultValueSql(CamusSequenceSyntax.NextValue(sequenceName));
        property.SetValueGeneratorFactory((p, _) => CreateSequenceValueGenerator(p, sequenceName, hiLo));
    }

    /// <summary>
    /// Reads the block size and the maximum from the sequence in the model that the generator serves,
    /// so a change of <c>HasSequence</c> after the property call is not lost.
    /// </summary>
    private static ValueGenerator CreateSequenceValueGenerator(IProperty property, string sequenceName, bool hiLo)
    {
        IReadOnlySequence? sequence = property.DeclaringType.Model.FindSequence(sequenceName);
        int blockSize = hiLo ? Math.Max(1, sequence?.IncrementBy ?? DefaultHiLoBlockSize) : 1;
        long? maxValue = sequence?.MaxValue;

        Type clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
        if (!IsSequenceValueType(clrType))
            throw new InvalidOperationException(
                $"Property '{property.DeclaringType.DisplayName()}.{property.Name}' draws from sequence " +
                $"'{sequenceName}' but has type '{property.ClrType.Name}'. Use long, int or short.");

        Type generatorType = typeof(CamusSequenceValueGenerator<>).MakeGenericType(property.ClrType);
        return (ValueGenerator)Activator.CreateInstance(generatorType, sequenceName, blockSize, maxValue)!;
    }

    /// <summary>The CLR types a sequence-backed property can have.</summary>
    internal static bool IsSequenceValueType(Type clrType)
        => clrType == typeof(long) || clrType == typeof(int) || clrType == typeof(short);
}
