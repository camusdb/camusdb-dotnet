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
                if (!SupportedConcurrencyTokenClrTypes.Contains(clrType))
                    throw new NotSupportedException(
                        $"CamusDB only supports [ConcurrencyCheck] on numeric columns (short, int, long). " +
                        $"Property '{entityType.DisplayName()}.{property.Name}' uses unsupported CLR type '{clrType.Name}'. " +
                        $"Note: [Timestamp] (byte[]) is not supported.");
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
