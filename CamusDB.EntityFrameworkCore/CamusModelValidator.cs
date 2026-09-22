using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CamusDB.EntityFrameworkCore;

public class CamusModelValidator : RelationalModelValidator
{
    private static readonly HashSet<Type> SupportedKeyClrTypes = new()
    {
        typeof(string),
        typeof(int),
        typeof(long),
        typeof(short),
        typeof(Guid),
    };

    public CamusModelValidator(
        ModelValidatorDependencies dependencies,
        RelationalModelValidatorDependencies relationalDependencies)
        : base(dependencies, relationalDependencies) { }

    public override void Validate(IModel model, IDiagnosticsLogger<DbLoggerCategory.Model.Validation> logger)
    {
        base.Validate(model, logger);
        ValidateConcurrencyTokens(model);
        ValidateComputedColumns(model);
        ValidateKeyTypes(model);
        ValidateColumnStorage(model);
        ValidateSequences(model);
    }

    private static readonly HashSet<Type> SupportedConcurrencyTokenClrTypes = new()
    {
        typeof(short),
        typeof(int),
        typeof(long),
    };

    private static void ValidateConcurrencyTokens(IModel model)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            foreach (var property in entityType.GetProperties())
            {
                if (!property.IsConcurrencyToken)
                    continue;

                var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;

                // [Timestamp] / IsRowVersion(): a byte[] token that the provider generates client-side
                // on add and update (see CamusRowVersionValueGenerator). Allowed as a rowversion only.
                if (clrType == typeof(byte[]))
                {
                    if (property.ValueGenerated == ValueGenerated.OnAddOrUpdate)
                        continue;

                    throw new NotSupportedException(
                        $"CamusDB supports a byte[] concurrency token only as a row version " +
                        $"([Timestamp] / IsRowVersion()). Property '{entityType.DisplayName()}.{property.Name}' " +
                        $"is a byte[] concurrency token that is not a row version.");
                }

                if (!SupportedConcurrencyTokenClrTypes.Contains(clrType))
                    throw new NotSupportedException(
                        $"CamusDB supports [ConcurrencyCheck] on numeric columns (short, int, long) and " +
                        $"[Timestamp]/IsRowVersion() on byte[]. " +
                        $"Property '{entityType.DisplayName()}.{property.Name}' uses unsupported CLR type '{clrType.Name}'.");
            }
        }
    }

    private static void ValidateComputedColumns(IModel model)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            foreach (var property in entityType.GetProperties())
            {
                if (property.GetComputedColumnSql() is not null)
                    throw new NotSupportedException(
                        $"CamusDB does not support computed columns. " +
                        $"Remove the computed column SQL from '{entityType.DisplayName()}.{property.Name}'.");
            }
        }
    }

    private static void ValidateKeyTypes(IModel model)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            foreach (var key in entityType.GetKeys())
            {
                foreach (var property in key.Properties)
                {
                    var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
                    if (!SupportedKeyClrTypes.Contains(clrType))
                        throw new NotSupportedException(
                            $"CamusDB does not support '{clrType.Name}' as a key type on " +
                            $"'{entityType.DisplayName()}.{property.Name}'. " +
                            $"Supported key types: string, int, long, short, Guid.");
                }
            }
        }
    }

    /// <summary>
    /// A storage strategy is refused on a column whose store type has no variable-length value. The
    /// server refuses the same DDL with <c>CADB0414</c> (<c>ColumnStorageNotApplicable</c>); refusing it
    /// here names the property and fails before a migration starts.
    /// </summary>
    private static void ValidateColumnStorage(IModel model)
    {
        foreach (var entityType in model.GetEntityTypes())
        {
            foreach (var property in entityType.GetProperties())
            {
                if (property.GetStorage() is not { } storage)
                    continue;

                string storeType = property.GetColumnType();
                if (!HasVariableLengthStoreType(storeType))
                    throw new NotSupportedException(
                        $"CamusDB supports a storage strategy only on string, bytes and array columns. " +
                        $"Property '{entityType.DisplayName()}.{property.Name}' has store type '{storeType}' " +
                        $"and storage strategy '{storage}'.");
            }
        }
    }

    /// <summary>
    /// A sequence must be one the server can create: an integer type, and not cyclic. A property that
    /// draws from a sequence must be an integer and name a sequence of the model. Both checks fail at
    /// model build, with the names of the sequence and the property, not at the first migration.
    /// </summary>
    private static void ValidateSequences(IModel model)
    {
        foreach (var sequence in model.GetSequences())
            CamusSequenceSyntax.EnsureSupported(sequence.Name, sequence.Type, sequence.IsCyclic);

        foreach (var entityType in model.GetEntityTypes())
        {
            foreach (var property in entityType.GetProperties())
            {
                string? sequenceName = property.GetSequenceName() ?? property.GetHiLoSequenceName();
                if (sequenceName is null)
                    continue;

                var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;
                if (!CamusPropertyBuilderExtensions.IsSequenceValueType(clrType))
                    throw new NotSupportedException(
                        $"Property '{entityType.DisplayName()}.{property.Name}' draws from sequence '{sequenceName}' " +
                        $"but has type '{clrType.Name}'. A sequence-backed property must be long, int or short.");

                if (model.FindSequence(sequenceName) is null)
                    throw new InvalidOperationException(
                        $"Property '{entityType.DisplayName()}.{property.Name}' draws from sequence '{sequenceName}', " +
                        "which is not in the model. Add it with modelBuilder.HasSequence.");
            }
        }
    }

    /// <summary><c>string</c>, <c>bytes</c> (alias <c>blob</c>) and <c>array(T)</c>, with or without a size.</summary>
    internal static bool HasVariableLengthStoreType(string storeType)
    {
        ReadOnlySpan<char> name = storeType.AsSpan().Trim();
        int paren = name.IndexOf('(');
        ReadOnlySpan<char> baseName = (paren < 0 ? name : name[..paren]).TrimEnd();

        return baseName.Equals("string", StringComparison.OrdinalIgnoreCase)
            || baseName.Equals("bytes", StringComparison.OrdinalIgnoreCase)
            || baseName.Equals("blob", StringComparison.OrdinalIgnoreCase)
            || baseName.Equals("array", StringComparison.OrdinalIgnoreCase);
    }
}
