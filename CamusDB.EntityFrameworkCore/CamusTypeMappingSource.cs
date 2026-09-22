using System.Data;
using System.Data.Common;
using CamusDB.Client;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusTypeMappingSource : RelationalTypeMappingSource
{
    /// <summary>
    /// Maps a <see cref="Guid"/> property to CamusDB's native <c>UUID</c> column. This is the default
    /// for a plain <see cref="Guid"/> property, and also the mapping for <c>HasColumnType("uuid")</c>.
    ///
    /// EF Core parameters only carry a <see cref="System.Data.DbType"/>, and <see cref="System.Data.DbType.Guid"/>
    /// resolves to <see cref="ColumnType.Id"/>, because the string-typed OID mapping sends object-id text
    /// that way. This mapping therefore stamps the wire column type directly on the
    /// <see cref="CamusParameter"/> so a UUID column is never confused with an OID key. A
    /// <see cref="Guid"/> value is sent as a Uuid in any case (see <c>CamusCommand.BuildColumnValue</c>),
    /// since a Guid can never be an object id.
    /// </summary>
    private sealed class CamusUuidTypeMapping : GuidTypeMapping
    {
        public CamusUuidTypeMapping() : base("uuid", System.Data.DbType.Guid) { }

        private CamusUuidTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters) { }

        protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
            => new CamusUuidTypeMapping(parameters);

        protected override void ConfigureParameter(DbParameter parameter)
        {
            base.ConfigureParameter(parameter);

            if (parameter is CamusParameter camusParameter)
                camusParameter.ColumnType = ColumnType.Uuid;
        }
    }

    private static readonly StringTypeMapping StringMapping = new("string", DbType.String);
    private static readonly BoolTypeMapping BoolMapping = new("bool");
    private static readonly ShortTypeMapping Int16Mapping = new("int64");
    private static readonly IntTypeMapping Int32Mapping = new("int64");
    private static readonly LongTypeMapping Int64Mapping = new("int64");
    private static readonly FloatTypeMapping Float32Mapping = new("float32", DbType.Single);
    private static readonly DoubleTypeMapping Float64Mapping = new("float64");
    private static readonly ByteArrayTypeMapping BytesMapping = new("bytes", DbType.Binary);
    // Date is exposed as both DateOnly (preferred) and DateTime; DateTime is exposed as DateTime/DateTimeOffset.
    private static readonly DateOnlyTypeMapping DateOnlyMapping = new("date", DbType.Date);
    private static readonly DateTimeTypeMapping DateAsDateTimeMapping = new("date", DbType.Date);
    private static readonly DateTimeTypeMapping DateTimeMapping = new("datetime", DbType.DateTime);
    private static readonly DateTimeOffsetTypeMapping DateTimeOffsetMapping = new("datetime", DbType.DateTime);
    // DbType.Guid → CamusParameter.FromDbType → ColumnType.Id (OID), while StringTypeMapping keeps
    // ClrType=string so the EF Core key-factory gets ValueComparer<string>, not DefaultValueComparer<Guid>.
    private static readonly StringTypeMapping IdStringMapping = new("id", DbType.Guid);
    // A Guid property declared HasColumnType("id"). Its parameter values are Guids, which the command
    // sends as Uuid values, because 16 bytes can never be a 12-byte object id; an OID column refuses
    // them, so a Guid property belongs in a UUID column.
    private static readonly GuidTypeMapping IdGuidMapping = new("id", DbType.Guid);
    // Native CamusDB UUID column: the default for a Guid property, and HasColumnType("uuid").
    private static readonly CamusUuidTypeMapping UuidMapping = new();

    // Native CamusDB ARRAY(T) columns. Each maps a CLR array type to a scalar element wire type.
    private static ValueComparer<T[]> ArrayComparer<T>() => new(
        (a, b) => (a == null && b == null) || (a != null && b != null && a.SequenceEqual(b)),
        a => a == null ? 0 : a.Aggregate(0, (h, v) => HashCode.Combine(h, v == null ? 0 : v.GetHashCode())),
        a => a == null ? null! : a.ToArray());

    private static readonly CamusArrayTypeMapping Int64ArrayMapping =
        CamusArrayTypeMapping.Create(typeof(long[]), "array(int64)", ColumnType.Integer64, Int64Mapping, ArrayComparer<long>());
    private static readonly CamusArrayTypeMapping StringArrayMapping =
        CamusArrayTypeMapping.Create(typeof(string[]), "array(string)", ColumnType.String, StringMapping, ArrayComparer<string>());
    private static readonly CamusArrayTypeMapping Float64ArrayMapping =
        CamusArrayTypeMapping.Create(typeof(double[]), "array(float64)", ColumnType.Float64, Float64Mapping, ArrayComparer<double>());
    private static readonly CamusArrayTypeMapping BoolArrayMapping =
        CamusArrayTypeMapping.Create(typeof(bool[]), "array(bool)", ColumnType.Bool, BoolMapping, ArrayComparer<bool>());

    private static readonly Dictionary<Type, RelationalTypeMapping> ArrayClrMappings = new()
    {
        { typeof(long[]), Int64ArrayMapping },
        { typeof(string[]), StringArrayMapping },
        { typeof(double[]), Float64ArrayMapping },
        { typeof(bool[]), BoolArrayMapping },
    };

    private static readonly Dictionary<string, RelationalTypeMapping> ArrayStoreMappings
        = new(StringComparer.OrdinalIgnoreCase)
    {
        { "array(int64)", Int64ArrayMapping },
        { "array(string)", StringArrayMapping },
        { "array(float64)", Float64ArrayMapping },
        { "array(bool)", BoolArrayMapping },
    };

    private static readonly Dictionary<Type, RelationalTypeMapping> ClrTypeMappings = new()
    {
        { typeof(string), StringMapping },
        { typeof(bool), BoolMapping },
        { typeof(short), Int16Mapping },
        { typeof(int), Int32Mapping },
        { typeof(long), Int64Mapping },
        { typeof(float), Float32Mapping },
        { typeof(double), Float64Mapping },
        { typeof(byte[]), BytesMapping },
        { typeof(DateOnly), DateOnlyMapping },
        { typeof(DateTime), DateTimeMapping },
        { typeof(DateTimeOffset), DateTimeOffsetMapping },
        { typeof(Guid), UuidMapping },
    };

    private static readonly Dictionary<string, RelationalTypeMapping> StoreTypeMappings
        = new(StringComparer.OrdinalIgnoreCase)
    {
        { "string", StringMapping },
        { "bool", BoolMapping },
        { "int64", Int64Mapping },
        { "float64", Float64Mapping },
        { "float32", Float32Mapping },
        { "real", Float32Mapping },
        { "bytes", BytesMapping },
        { "blob", BytesMapping },
        { "id",  IdStringMapping },
        { "oid", IdStringMapping },
        { "uuid", UuidMapping },
        { "guid", UuidMapping },
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

        // Native CamusDB ARRAY(T) columns: match by explicit store type first, then by CLR array type.
        // Resolving a direct mapping here (rather than leaving arrays to EF's primitive-collection
        // convention) is what routes them to a real ARRAY column instead of a JSON string.
        if (storeTypeName is not null && ArrayStoreMappings.TryGetValue(storeTypeName, out var arrayByStore))
            return arrayByStore;

        if (storeTypeName is null && clrType is not null && ArrayClrMappings.TryGetValue(clrType, out var arrayByClr))
            return arrayByClr;

        // "id"/"oid" store type: pick string or Guid mapping based on the CLR property type.
        // Without this check, a string-typed primary key with HasColumnType("id") would receive
        // a GuidTypeMapping and its DefaultValueComparer<Guid> would crash the key factory.
        if (storeTypeName is not null &&
            (storeTypeName.Equals("id", StringComparison.OrdinalIgnoreCase) ||
             storeTypeName.Equals("oid", StringComparison.OrdinalIgnoreCase)))
        {
            return clrType == typeof(Guid) ? IdGuidMapping : IdStringMapping;
        }

        // Native "uuid"/"guid" store type: use the Guid mapping unless the property is string-typed
        // (in which case keep a string mapping so the value round-trips as the canonical UUID text).
        if (storeTypeName is not null &&
            (storeTypeName.Equals("uuid", StringComparison.OrdinalIgnoreCase) ||
             storeTypeName.Equals("guid", StringComparison.OrdinalIgnoreCase)))
        {
            return clrType == typeof(string) ? new StringTypeMapping("uuid", DbType.String) : UuidMapping;
        }

        // "date"/"datetime"/"timestamp" store types: pick the mapping matching the CLR property type
        // so a DateTime-typed property declared HasColumnType("date") still gets a DateTime mapping
        // (its DbType.Date routes through CamusParameter to ColumnType.Date).
        if (storeTypeName is not null &&
            storeTypeName.Equals("date", StringComparison.OrdinalIgnoreCase))
        {
            return clrType == typeof(DateTime) ? DateAsDateTimeMapping : DateOnlyMapping;
        }

        if (storeTypeName is not null &&
            (storeTypeName.Equals("datetime", StringComparison.OrdinalIgnoreCase) ||
             storeTypeName.Equals("timestamp", StringComparison.OrdinalIgnoreCase)))
        {
            return clrType == typeof(DateTimeOffset) ? DateTimeOffsetMapping : DateTimeMapping;
        }

        if (storeTypeName is not null && StoreTypeMappings.TryGetValue(storeTypeName, out var storeMapping))
            return storeMapping;

        if (clrType is not null && ClrTypeMappings.TryGetValue(clrType, out var clrMapping))
            return clrMapping;

        return null;
    }
}
