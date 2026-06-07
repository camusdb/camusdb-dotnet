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
    // DbType.Guid → CamusParameter.FromDbType → ColumnType.Id (OID), while StringTypeMapping keeps
    // ClrType=string so the EF Core key-factory gets ValueComparer<string>, not DefaultValueComparer<Guid>.
    private static readonly StringTypeMapping IdStringMapping = new("id", DbType.Guid);
    private static readonly GuidTypeMapping IdGuidMapping = new("id", DbType.Guid);

    private static readonly Dictionary<Type, RelationalTypeMapping> ClrTypeMappings = new()
    {
        { typeof(string), StringMapping },
        { typeof(bool), BoolMapping },
        { typeof(short), Int16Mapping },
        { typeof(int), Int32Mapping },
        { typeof(long), Int64Mapping },
        { typeof(float), Float32Mapping },
        { typeof(double), Float64Mapping },
        { typeof(Guid), IdGuidMapping },
    };

    private static readonly Dictionary<string, RelationalTypeMapping> StoreTypeMappings
        = new(StringComparer.OrdinalIgnoreCase)
    {
        { "string", StringMapping },
        { "bool", BoolMapping },
        { "int64", Int64Mapping },
        { "float64", Float64Mapping },
        { "id",  IdStringMapping },
        { "oid", IdStringMapping },
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
        var clrType = mappingInfo.ClrType;

        // "id"/"oid" store type: pick string or Guid mapping based on the CLR property type.
        // Without this check, a string-typed primary key with HasColumnType("id") would receive
        // a GuidTypeMapping and its DefaultValueComparer<Guid> would crash the key factory.
        if (storeTypeName is not null &&
            (storeTypeName.Equals("id", StringComparison.OrdinalIgnoreCase) ||
             storeTypeName.Equals("oid", StringComparison.OrdinalIgnoreCase)))
        {
            return clrType == typeof(Guid) ? IdGuidMapping : IdStringMapping;
        }

        if (storeTypeName is not null && StoreTypeMappings.TryGetValue(storeTypeName, out var storeMapping))
            return storeMapping;

        if (clrType is not null && ClrTypeMappings.TryGetValue(clrType, out var clrMapping))
            return clrMapping;

        return null;
    }
}
