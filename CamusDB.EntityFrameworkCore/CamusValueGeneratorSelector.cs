using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.ValueGeneration;
using Microsoft.EntityFrameworkCore.ValueGeneration.Internal;

namespace CamusDB.EntityFrameworkCore;

public class CamusValueGeneratorSelector : RelationalValueGeneratorSelector
{
    public CamusValueGeneratorSelector(ValueGeneratorSelectorDependencies dependencies)
        : base(dependencies) { }

    public override bool TrySelect(IProperty property, ITypeBase typeBase, [NotNullWhen(true)] out ValueGenerator? valueGenerator)
    {
        // Return client-side ObjectId generator for string key properties mapped to "id" store type
        if (property.ClrType == typeof(string)
            && property.ValueGenerated != ValueGenerated.Never)
        {
            var storeType = property.GetColumnType();
            if (string.Equals(storeType, "id", StringComparison.OrdinalIgnoreCase)
                || string.Equals(storeType, "oid", StringComparison.OrdinalIgnoreCase))
            {
                valueGenerator = new CamusObjectIdValueGenerator();
                return true;
            }
        }

        return base.TrySelect(property, typeBase, out valueGenerator);
    }
}
