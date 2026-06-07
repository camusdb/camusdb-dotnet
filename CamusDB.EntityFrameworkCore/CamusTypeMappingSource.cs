using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusTypeMappingSource : RelationalTypeMappingSource
{
    private static readonly StringTypeMapping StringMapping = new("string", DbType.String);
    private static readonly BoolTypeMapping BoolMapping = new("bool");
    private static readonly ShortTypeMapping Int16Mapping = new("int64");
    private static readonly IntTypeMapping Int32Mapping = new("int64");
    private static readonly LongTypeMapping Int64Mapping = new("int64");
    private static readonly FloatTypeMapping Float32Mapping = new("float64");
    private static readonly DoubleTypeMapping Float64Mapping = new("float64");
    private static readonly GuidTypeMapping IdMapping = new("id", DbType.String);

    private static readonly Dictionary<Type, RelationalTypeMapping> ClrTypeMappings = new()
    {
        { typeof(string), StringMapping },
        { typeof(bool), BoolMapping },
        { typeof(short), Int16Mapping },
        { typeof(int), Int32Mapping },
        { typeof(long), Int64Mapping },
        { typeof(float), Float32Mapping },
        { typeof(double), Float64Mapping },
        { typeof(Guid), IdMapping },
    };

    private static readonly Dictionary<string, RelationalTypeMapping> StoreTypeMappings
        = new(StringComparer.OrdinalIgnoreCase)
    {
        { "string", StringMapping },
        { "bool", BoolMapping },
        { "int64", Int64Mapping },
        { "float64", Float64Mapping },
        { "id", IdMapping },
    };

    public CamusTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var storeTypeName = mappingInfo.StoreTypeName;
        if (storeTypeName is not null && StoreTypeMappings.TryGetValue(storeTypeName, out var storeMapping))
            return storeMapping;

        var clrType = mappingInfo.ClrType;
        if (clrType is not null && ClrTypeMappings.TryGetValue(clrType, out var clrMapping))
            return clrMapping;

        return null;
    }
}
