
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Data.Common;

namespace CamusDB.Client;

/// <summary>
/// Represents a collection of parameters associated with a <see cref="CamusCommand" /> and their
/// respective mappings to columns in a DataSet.
/// </summary>
public sealed class CamusParameterCollection : DbParameterCollection, IEnumerable<CamusParameter>
{
    private readonly List<CamusParameter> _innerList = new List<CamusParameter>();

    /// <inheritdoc />
    public override int Count => _innerList.Count;

    /// <inheritdoc />
    public override object SyncRoot => _innerList;

    /// <inheritdoc />
    IEnumerator<CamusParameter> IEnumerable<CamusParameter>.GetEnumerator() => _innerList.GetEnumerator();

    /// <inheritdoc />
    public override IEnumerator GetEnumerator() => _innerList.GetEnumerator();

    /// <inheritdoc />
    public override int Add(object value)
    {
        if (value == null)        
            throw new ArgumentNullException(nameof(value));        

        _innerList.Add((CamusParameter)value);
        return _innerList.Count - 1;
    }

    public CamusParameter Add(string parameterName, ColumnType dbType)
    {
        CamusParameter parameter = new CamusParameter(parameterName, dbType);
        _innerList.Add(parameter);
        return parameter;
    }

    public CamusParameter Add(string parameterName, ColumnType dbType, object? value)
    {
        CamusParameter parameter = Add(parameterName, dbType);
        parameter.Value = value;
        return parameter;
    }

    public override void AddRange(Array values)
    {
        _innerList.AddRange(values.Cast<CamusParameter>());
    }

    public override void Clear()
    {
        _innerList.Clear();
    }

    public override bool Contains(object value)
    {
        return value is CamusParameter parameter && _innerList.Contains(parameter);
    }

    public override bool Contains(string value)
    {
        return IndexOf(value) >= 0;
    }

    public override void CopyTo(Array array, int index)
    {
        for (int i = 0; i < _innerList.Count; i++)
            array.SetValue(_innerList[i], index + i);
    }

    public override int IndexOf(object value)
    {
        return value is CamusParameter parameter ? _innerList.IndexOf(parameter) : -1;
    }

    public override int IndexOf(string parameterName)
    {
        return _innerList.FindIndex(x => string.Equals(x.ParameterName, parameterName, StringComparison.Ordinal));
    }

    public override void Insert(int index, object value)
    {
        _innerList.Insert(index, ValidateParameter(value));
    }

    public override void Remove(object value)
    {
        if (value is CamusParameter parameter)
            _innerList.Remove(parameter);
    }

    public override void RemoveAt(int index)
    {
        _innerList.RemoveAt(index);
    }

    public override void RemoveAt(string parameterName)
    {
        int index = IndexOf(parameterName);
        if (index >= 0)
            _innerList.RemoveAt(index);
    }

    protected override DbParameter GetParameter(int index)
    {
        return _innerList[index];
    }

    protected override DbParameter GetParameter(string parameterName)
    {
        int index = IndexOf(parameterName);
        if (index < 0)
            throw new IndexOutOfRangeException($"Parameter '{parameterName}' was not found.");

        return _innerList[index];
    }

    protected override void SetParameter(int index, DbParameter value)
    {
        _innerList[index] = ValidateParameter(value);
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        int index = IndexOf(parameterName);
        if (index < 0)
            throw new IndexOutOfRangeException($"Parameter '{parameterName}' was not found.");

        _innerList[index] = ValidateParameter(value);
    }

    private static CamusParameter ValidateParameter(object value)
    {
        if (value is null)
            throw new ArgumentNullException(nameof(value));

        if (value is not CamusParameter parameter)
            throw new ArgumentException("Value must be a CamusParameter.", nameof(value));

        return parameter;
    }
}
