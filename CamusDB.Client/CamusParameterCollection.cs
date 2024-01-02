
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
        {
            throw new ArgumentNullException(nameof(value));
        }

        _innerList.Add((CamusParameter)value);
        return _innerList.Count - 1;
    }

    public CamusParameter Add(string parameterName, ColumnType dbType)
    {
        var parameter = new CamusParameter(parameterName, dbType);
        _innerList.Add(parameter);
        return parameter;
    }

    public CamusParameter Add(string parameterName, ColumnType dbType, object value)
    {
        var parameter = Add(parameterName, dbType);
        parameter.Value = value;
        return parameter;
    }

    public override void AddRange(Array values)
    {
        _innerList.AddRange(values.Cast<CamusParameter>());
    }

    public override void Clear()
    {
        throw new NotImplementedException();
    }

    public override bool Contains(object value)
    {
        throw new NotImplementedException();
    }

    public override bool Contains(string value)
    {
        throw new NotImplementedException();
    }

    public override void CopyTo(Array array, int index)
    {
        throw new NotImplementedException();
    }

    public override int IndexOf(object value)
    {
        throw new NotImplementedException();
    }

    public override int IndexOf(string parameterName)
    {
        throw new NotImplementedException();
    }

    public override void Insert(int index, object value)
    {
        throw new NotImplementedException();
    }

    public override void Remove(object value)
    {
        throw new NotImplementedException();
    }

    public override void RemoveAt(int index)
    {
        throw new NotImplementedException();
    }

    public override void RemoveAt(string parameterName)
    {
        throw new NotImplementedException();
    }

    protected override DbParameter GetParameter(int index)
    {
        throw new NotImplementedException();
    }

    protected override DbParameter GetParameter(string parameterName)
    {
        throw new NotImplementedException();
    }

    protected override void SetParameter(int index, DbParameter value)
    {
        throw new NotImplementedException();
    }

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        throw new NotImplementedException();
    }
}
