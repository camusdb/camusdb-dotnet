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
}
