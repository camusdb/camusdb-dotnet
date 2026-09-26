/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using CamusDB.Client;
using CamusDB.Core.Util.ObjectIds;
using Dapper;

namespace CamusDB.Dapper;

/// <summary>
/// A parameter value with an explicit CamusDB <see cref="ColumnType"/>, for the values a
/// <see cref="DbType"/> cannot express.
///
/// <para>Dapper types a parameter by its CLR type. That covers most columns, but not three cases: an
/// object id held as a <see cref="string"/> (Dapper sends it as a <c>STRING</c>), a native
/// <c>ARRAY(T)</c> (Dapper expands any <see cref="System.Collections.IEnumerable"/> into an
/// <c>IN (@p1, @p2, …)</c> list), and a typed <c>NULL</c> or empty array. Wrap the value in a
/// <see cref="CamusValue"/> to bind it as the column type you name:</para>
///
/// <code>
/// await connection.QueryAsync&lt;Robot&gt;(
///     "SELECT * FROM robots WHERE year = ANY(@years)",
///     new { years = CamusValue.Array(new long[] { 1977, 1984 }) });
/// </code>
///
/// <para>Dapper calls <see cref="SqlMapper.ICustomQueryParameter.AddParameter"/> for a value of this
/// type, so it works as a member of an anonymous object and as a value in a
/// <see cref="DynamicParameters"/>.</para>
/// </summary>
public sealed class CamusValue : SqlMapper.ICustomQueryParameter
{
    /// <summary>The column type the value binds as.</summary>
    public ColumnType Type { get; }

    /// <summary>The value, or <see langword="null"/> for a SQL <c>NULL</c>.</summary>
    public object? Value { get; }

    /// <summary>
    /// For an <see cref="ColumnType.Array"/> value, the scalar element type. <see cref="ColumnType.Null"/>
    /// lets the driver infer it from the elements, which fails for an empty array.
    /// </summary>
    public ColumnType ArrayElementType { get; }

    /// <summary>Creates a value that binds as <paramref name="type"/>.</summary>
    public CamusValue(ColumnType type, object? value, ColumnType arrayElementType = ColumnType.Null)
    {
        if (type != ColumnType.Array && arrayElementType != ColumnType.Null)
            throw new ArgumentException("An element type applies to an array value only.", nameof(arrayElementType));

        Type = type;
        Value = value;
        ArrayElementType = arrayElementType;
    }

    /// <summary>An object id (<c>OID</c>) in its 24-character hex form.</summary>
    public static CamusValue Id(string? value) => new(ColumnType.Id, value);

    /// <summary>An object id (<c>OID</c>).</summary>
    public static CamusValue Id(CamusObjectIdValue? value) => new(ColumnType.Id, value?.ToString());

    /// <summary>A <c>UUID</c>.</summary>
    public static CamusValue Uuid(Guid? value) => new(ColumnType.Uuid, value);

    /// <summary>
    /// A native <c>ARRAY(T)</c>. The element type comes from <typeparamref name="T"/>, so an empty
    /// array binds with the correct type too. A <see langword="null"/> sequence binds as <c>NULL</c>.
    /// </summary>
    public static CamusValue Array<T>(IEnumerable<T>? values)
        => new(ColumnType.Array, values, ElementTypeOf(typeof(T)));

    /// <summary>A native <c>ARRAY(T)</c> with an explicit element type, for example
    /// <see cref="ColumnType.Id"/> for an array of object ids held as strings.</summary>
    public static CamusValue Array<T>(IEnumerable<T>? values, ColumnType elementType)
        => new(ColumnType.Array, values, elementType);

    /// <summary>
    /// A float32 vector for a <c>BYTES</c> column, packed in the layout <see cref="CamusVector"/> defines.
    /// </summary>
    public static CamusValue Vector(ReadOnlySpan<float> vector) => new(ColumnType.Bytes, CamusVector.ToBytes(vector));

    void SqlMapper.ICustomQueryParameter.AddParameter(IDbCommand command, string name)
    {
        CamusParameter parameter = new(name, Type, Value ?? DBNull.Value)
        {
            ArrayElementType = ArrayElementType,
        };

        command.Parameters.Add(parameter);
    }

    internal static ColumnType ElementTypeOf(Type type)
    {
        Type elementType = Nullable.GetUnderlyingType(type) ?? type;

        ColumnType columnType = CamusCommand.InferColumnType(elementType);

        if (columnType == ColumnType.Null)
            throw new NotSupportedException(
                $"CamusDB has no array element type for {elementType.Name}. Pass the element type explicitly.");

        return columnType;
    }
}
