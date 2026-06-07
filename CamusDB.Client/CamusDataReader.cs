/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections;
using System.Data.Common;
using System.Globalization;

namespace CamusDB.Client;

public class CamusDataReader : DbDataReader
{
    private int position = -1;

    private bool isClosed;

    private readonly List<Dictionary<string, ColumnValue>> rows;

    private readonly string[] columnNames;

    public override int Depth => 0;

    public override int FieldCount => columnNames.Length;

    public override bool HasRows => rows.Count > 0;

    public override bool IsClosed => isClosed;

    private readonly int recordsAffected = -1;

    public override int RecordsAffected => recordsAffected;

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override object this[int ordinal] => GetValue(ordinal);

    public CamusDataReader(List<Dictionary<string, ColumnValue>> rows)
    {
        this.rows = rows;
        columnNames = rows.Count > 0 ? rows[0].Keys.ToArray() : Array.Empty<string>();
    }

    public CamusDataReader(int recordsAffected)
    {
        this.rows = [];
        this.columnNames = [];
        this.recordsAffected = recordsAffected;
    }

    public override void Close()
    {
        isClosed = true;
    }

    public override bool Read()
    {
        ThrowIfClosed();

        position++;
        return position < rows.Count;
    }

    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Read());
    }

    public override bool NextResult() => false;

    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < columnNames.Length; i++)
        {
            if (string.Equals(columnNames[i], name, StringComparison.Ordinal))
                return i;
        }

        throw new IndexOutOfRangeException($"Column '{name}' was not found.");
    }

    public override string GetName(int ordinal) => columnNames[ordinal];

    public override string GetDataTypeName(int ordinal) => GetColumnValue(ordinal).Type.ToString();

    public override Type GetFieldType(int ordinal) => GetColumnValue(ordinal).Type switch
    {
        ColumnType.Bool => typeof(bool),
        ColumnType.Float64 => typeof(double),
        ColumnType.Id => typeof(string),
        ColumnType.Integer64 => typeof(long),
        ColumnType.Null => typeof(DBNull),
        ColumnType.String => typeof(string),
        _ => typeof(object)
    };

    public override object GetValue(int ordinal)
    {
        ColumnValue value = GetColumnValue(ordinal);

        return value.Type switch
        {
            ColumnType.Bool => value.BoolValue,
            ColumnType.Float64 => value.FloatValue,
            ColumnType.Id => value.StrValue ?? "",
            ColumnType.Integer64 => value.LongValue,
            ColumnType.Null => DBNull.Value,
            ColumnType.String => value.StrValue ?? "",
            _ => DBNull.Value
        };
    }

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);

        for (int i = 0; i < count; i++)
            values[i] = GetValue(i);

        return count;
    }

    public override bool IsDBNull(int ordinal) => GetColumnValue(ordinal).Type == ColumnType.Null;

    public override bool GetBoolean(int ordinal) => GetColumnValue(ordinal).Type switch
    {
        ColumnType.Bool => GetColumnValue(ordinal).BoolValue,
        ColumnType.Integer64 => GetColumnValue(ordinal).LongValue != 0,
        _ => throw new InvalidCastException()
    };

    public override byte GetByte(int ordinal)
    {
        long value = GetInt64(ordinal);

        if (value < byte.MinValue || value > byte.MaxValue)
            throw new OverflowException();

        return (byte)value;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        byte[] data = System.Text.Encoding.UTF8.GetBytes(GetString(ordinal));
        return CopyBuffer(data, dataOffset, buffer, bufferOffset, length);
    }

    public override char GetChar(int ordinal)
    {
        string value = GetString(ordinal);

        if (value.Length != 1)
            throw new InvalidCastException();

        return value[0];
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        char[] data = GetString(ordinal).ToCharArray();
        return CopyBuffer(data, dataOffset, buffer, bufferOffset, length);
    }

    public override DateTime GetDateTime(int ordinal)
    {
        object value = GetValue(ordinal);

        if (value is DateTime dateTime)
            return dateTime;

        return DateTime.Parse(Convert.ToString(value, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture);
    }

    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal), CultureInfo.InvariantCulture);

    public override double GetDouble(int ordinal) => GetColumnValue(ordinal).Type switch
    {
        ColumnType.Float64 => GetColumnValue(ordinal).FloatValue,
        ColumnType.Integer64 => GetColumnValue(ordinal).LongValue,
        _ => throw new InvalidCastException()
    };

    public override IEnumerator GetEnumerator()
    {
        while (Read())
            yield return this;
    }

    public override float GetFloat(int ordinal) => Convert.ToSingle(GetDouble(ordinal), CultureInfo.InvariantCulture);

    public override Guid GetGuid(int ordinal)
    {
        string value = GetString(ordinal);
        return Guid.Parse(value);
    }

    public override short GetInt16(int ordinal)
    {
        long value = GetInt64(ordinal);

        if (value < short.MinValue || value > short.MaxValue)
            throw new OverflowException();

        return (short)value;
    }

    public override int GetInt32(int ordinal)
    {
        long value = GetInt64(ordinal);

        if (value < int.MinValue || value > int.MaxValue)
            throw new OverflowException();

        return (int)value;
    }

    public override long GetInt64(int ordinal)
    {
        ColumnValue value = GetColumnValue(ordinal);

        return value.Type switch
        {
            ColumnType.Integer64 => value.LongValue,
            ColumnType.Float64 => checked((long)value.FloatValue),
            _ => throw new InvalidCastException()
        };
    }

    public override string GetString(int ordinal)
    {
        ColumnValue value = GetColumnValue(ordinal);

        return value.Type switch
        {
            ColumnType.Id => value.StrValue ?? "",
            ColumnType.String => value.StrValue ?? "",
            _ => Convert.ToString(GetValue(ordinal), CultureInfo.InvariantCulture) ?? ""
        };
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            Close();

        base.Dispose(disposing);
    }

    private Dictionary<string, ColumnValue> GetCurrent()
    {
        ThrowIfClosed();

        if (position < 0 || position >= rows.Count)
            throw new InvalidOperationException("No current row is available.");

        return rows[position];
    }

    private ColumnValue GetColumnValue(int ordinal)
    {
        if (ordinal < 0 || ordinal >= FieldCount)
            throw new IndexOutOfRangeException();

        string columnName = columnNames[ordinal];
        Dictionary<string, ColumnValue> current = GetCurrent();

        if (!current.TryGetValue(columnName, out ColumnValue? value))
            throw new IndexOutOfRangeException($"Column '{columnName}' was not found in the current row.");

        return value;
    }

    private void ThrowIfClosed()
    {
        if (isClosed)
            throw new InvalidOperationException("The data reader is closed.");
    }

    private static long CopyBuffer<T>(T[] data, long dataOffset, T[]? buffer, int bufferOffset, int length)
    {
        if (dataOffset < 0)
            throw new ArgumentOutOfRangeException(nameof(dataOffset));

        if (dataOffset >= data.Length)
            return 0;

        int available = data.Length - (int)dataOffset;
        int copied = Math.Min(available, length);

        if (buffer is not null)
            Array.Copy(data, (int)dataOffset, buffer, bufferOffset, copied);

        return copied;
    }
}
