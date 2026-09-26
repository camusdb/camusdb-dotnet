/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Data;
using System.Globalization;
using CamusDB.Client;
using CamusDB.Core.Util.ObjectIds;
using Dapper;

namespace CamusDB.Dapper;

/// <summary>
/// Binds a <see cref="CamusObjectIdValue"/> as an <c>OID</c> and reads an <c>OID</c> column into one.
/// </summary>
public sealed class CamusObjectIdTypeHandler : SqlMapper.TypeHandler<CamusObjectIdValue>
{
    /// <inheritdoc />
    public override void SetValue(IDbDataParameter parameter, CamusObjectIdValue value)
    {
        CamusTypeHandlers.SetColumnType(parameter, ColumnType.Id);
        parameter.Value = value.ToString();
    }

    /// <inheritdoc />
    public override CamusObjectIdValue Parse(object value) => value switch
    {
        CamusObjectIdValue id => id,
        string text => CamusObjectIdValue.ToValue(text),
        _ => throw new DataException($"Cannot read an object id from {value.GetType().Name}.")
    };
}

/// <summary>
/// Binds a <typeparamref name="T"/>[] as a native <c>ARRAY(T)</c> and reads an <c>ARRAY(T)</c> column
/// into one. Register it through <see cref="CamusDapper.RegisterArrayTypeHandler{T}"/>.
///
/// <para>A type handler for <typeparamref name="T"/>[] replaces Dapper's list expansion for that type:
/// <c>WHERE x IN @values</c> stops working for it, and <c>WHERE x = ANY(@values)</c> takes its place.
/// </para>
/// </summary>
public sealed class CamusArrayTypeHandler<T> : SqlMapper.TypeHandler<T[]>
{
    private readonly ColumnType elementType;

    /// <summary>Creates a handler whose element type comes from <typeparamref name="T"/>.</summary>
    public CamusArrayTypeHandler() : this(CamusValue.ElementTypeOf(typeof(T))) { }

    /// <summary>Creates a handler with an explicit element type.</summary>
    public CamusArrayTypeHandler(ColumnType elementType)
    {
        this.elementType = elementType;
    }

    /// <inheritdoc />
    public override void SetValue(IDbDataParameter parameter, T[]? value)
    {
        CamusTypeHandlers.SetColumnType(parameter, ColumnType.Array);

        if (parameter is CamusParameter camusParameter)
            camusParameter.ArrayElementType = elementType;

        parameter.Value = value is null ? DBNull.Value : value;
    }

    /// <inheritdoc />
    public override T[] Parse(object value)
    {
        if (value is T[] typed)
            return typed;

        if (value is not object?[] elements)
            throw new DataException($"Cannot read a {typeof(T).Name}[] from {value.GetType().Name}.");

        T[] result = new T[elements.Length];

        for (int i = 0; i < elements.Length; i++)
            result[i] = ConvertElement(elements[i]);

        return result;
    }

    private static T ConvertElement(object? element)
    {
        if (element is null or DBNull)
            return default!;

        if (element is T typed)
            return typed;

        Type target = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

        // The reader returns a DATE element as a DateTime at midnight UTC, as it does a DATE column.
        if (target == typeof(DateOnly) && element is DateTime dateTime)
            return (T)(object)DateOnly.FromDateTime(dateTime);

        if (target == typeof(Guid) && element is string text)
            return (T)(object)Guid.Parse(text);

        return (T)Convert.ChangeType(element, target, CultureInfo.InvariantCulture);
    }
}

internal static class CamusTypeHandlers
{
    /// <summary>
    /// Sets the CamusDB column type of a parameter. Dapper creates each parameter through the command,
    /// so on a CamusDB connection it is always a <see cref="CamusParameter"/>. Any other parameter is
    /// left to its <see cref="DbType"/>.
    /// </summary>
    internal static void SetColumnType(IDbDataParameter parameter, ColumnType columnType)
    {
        if (parameter is CamusParameter camusParameter)
            camusParameter.ColumnType = columnType;
    }
}
