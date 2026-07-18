
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Data;
using System.Data.Common;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage;
using CamusDB.Client;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Maps a CLR array (e.g. <c>long[]</c>, <c>string[]</c>) to a CamusDB native <c>ARRAY(T)</c> column.
/// CamusDB stores arrays as a first-class column type carried on the parameter as
/// <see cref="ColumnType.Array"/> with a scalar <see cref="CamusParameter.ArrayElementType"/>; unlike
/// EF's default primitive-collection support, no JSON string round-trip is involved. Arrays are not
/// indexable and have no SQL literal form, so this mapping is for storage/retrieval only.
/// </summary>
public sealed class CamusArrayTypeMapping : RelationalTypeMapping
{
    public ColumnType ElementType { get; }

    private CamusArrayTypeMapping(RelationalTypeMappingParameters parameters, ColumnType elementType)
        : base(parameters)
    {
        ElementType = elementType;
    }

    public static CamusArrayTypeMapping Create(
        Type clrArrayType,
        string storeType,
        ColumnType elementType,
        RelationalTypeMapping elementMapping,
        ValueComparer comparer)
        => new(
            new RelationalTypeMappingParameters(
                // Carrying the scalar element mapping makes this a proper collection type mapping. It does
                // not change how an ARRAY *column* is stored (ConfigureParameter still stamps
                // ColumnType.Array), but it is what EF's primitive-collection machinery needs to render a
                // parameterized `col IN (…)` predicate — e.g. `values.Contains(entity.Col)` — by expanding
                // the collection through the element mapping. Without it EF builds a collection parameter
                // with no type mapping and throws NullReferenceException while creating the DbCommand.
                new CoreTypeMappingParameters(clrArrayType, comparer: comparer, keyComparer: comparer, elementMapping: elementMapping),
                storeType,
                StoreTypePostfix.None,
                System.Data.DbType.Object),
            elementType);

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new CamusArrayTypeMapping(parameters, ElementType);

    protected override void ConfigureParameter(DbParameter parameter)
    {
        base.ConfigureParameter(parameter);

        if (parameter is CamusParameter camusParameter)
        {
            camusParameter.ColumnType = ColumnType.Array;
            camusParameter.ArrayElementType = ElementType;
        }
    }

    // Arrays have no SQL literal form in CamusDB — they can only travel as a bound parameter.
    protected override string GenerateNonNullSqlLiteral(object value)
        => throw new NotSupportedException(
            "CamusDB array columns have no SQL literal form; use a bound parameter (they cannot appear " +
            "as inline literals in migrations seed data or default values).");
}
