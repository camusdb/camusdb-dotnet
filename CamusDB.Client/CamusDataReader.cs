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

    private readonly CamusResultSet resultSet;

    private readonly string[] columnNames;

    public override int Depth => 0;

    public override int FieldCount => columnNames.Length;

    public override bool HasRows => resultSet.RowCount > 0;

    public override bool IsClosed => isClosed;

    private readonly int recordsAffected = -1;

    public override int RecordsAffected => recordsAffected;

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override object this[int ordinal] => GetValue(ordinal);

    /// <summary>
    /// Query result cache resolution for the <c>SELECT</c> that produced this reader, or
    /// <see langword="null"/> when the query carried no <c>{cache=…}</c> hint.
    /// </summary>
    public CamusCacheMetadata? CacheMetadata { get; }

    public CamusDataReader(CamusResultSet resultSet, CamusCacheMetadata? cacheMetadata = null)
    {
        this.resultSet = resultSet;
        columnNames = resultSet.ColumnNames;
        CacheMetadata = cacheMetadata;
    }

    public CamusDataReader(int recordsAffected)
    {
        this.resultSet = CamusResultSet.Empty;
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
        return position < resultSet.RowCount;
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
        ColumnType.Float32 => typeof(float),
        ColumnType.Id => typeof(string),
        ColumnType.Integer64 => typeof(long),
        ColumnType.Bytes => typeof(byte[]),
        ColumnType.Date => typeof(DateTime),
        ColumnType.DateTime => typeof(DateTime),
        ColumnType.Array => typeof(object[]),
        ColumnType.Uuid => typeof(Guid),
        ColumnType.Null => typeof(DBNull),
        ColumnType.String => typeof(string),
        _ => typeof(object)
    };

    public override object GetValue(int ordinal) => ConvertToClr(GetColumnValue(ordinal));

    // ColumnValue is a sizeable struct; take it by 'in' to avoid copying it on the per-cell path.
    private static object ConvertToClr(in ColumnValue value) => value.Type switch
    {
        ColumnType.Bool => value.BoolValue,
        ColumnType.Float64 => value.FloatValue,
        ColumnType.Float32 => (float)value.FloatValue,
        ColumnType.Id => value.StrValue ?? "",
        ColumnType.Integer64 => value.LongValue,
        ColumnType.Bytes => value.BytesValue ?? Array.Empty<byte>(),
        ColumnType.Date => new DateTime(value.LongValue, DateTimeKind.Utc),
        ColumnType.DateTime => new DateTime(value.LongValue, DateTimeKind.Utc),
        ColumnType.Array => ConvertArray(in value),
        ColumnType.Uuid => value.AsGuid(),
        ColumnType.Null => DBNull.Value,
        ColumnType.String => value.StrValue ?? "",
        _ => DBNull.Value
    };

    private static object?[] ConvertArray(in ColumnValue value)
    {
        List<ColumnValue> elements = value.ArrayValues ?? [];
        object?[] result = new object?[elements.Count];

        for (int i = 0; i < elements.Count; i++)
            result[i] = elements[i].Type == ColumnType.Null ? null : ConvertToClr(elements[i]);

        return result;
    }

    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);

        for (int i = 0; i < count; i++)
            values[i] = GetValue(i);

        return count;
    }

    public override bool IsDBNull(int ordinal) => GetColumnValue(ordinal).Type == ColumnType.Null;

    public override bool GetBoolean(int ordinal)
    {
        ColumnValue value = GetColumnValue(ordinal);

        return value.Type switch
        {
            ColumnType.Bool => value.BoolValue,
            ColumnType.Integer64 => value.LongValue != 0,
            _ => throw new InvalidCastException()
        };
    }

    public override byte GetByte(int ordinal)
    {
        long value = GetInt64(ordinal);

        if (value < byte.MinValue || value > byte.MaxValue)
            throw new OverflowException();

        return (byte)value;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        ColumnValue value = GetColumnValue(ordinal);
        byte[] data = value.Type == ColumnType.Bytes
            ? value.BytesValue ?? []
            : System.Text.Encoding.UTF8.GetBytes(GetString(ordinal));
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
        ColumnValue column = GetColumnValue(ordinal);

        if (column.Type is ColumnType.Date or ColumnType.DateTime)
            return new DateTime(column.LongValue, DateTimeKind.Utc);

        object value = GetValue(ordinal);

        if (value is DateTime dateTime)
            return dateTime;

        return DateTime.Parse(
            Convert.ToString(value, CultureInfo.InvariantCulture)!,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
    }

    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal), CultureInfo.InvariantCulture);

    public override double GetDouble(int ordinal)
    {
        ColumnValue value = GetColumnValue(ordinal);

        return value.Type switch
        {
            ColumnType.Float64 => value.FloatValue,
            ColumnType.Float32 => (float)value.FloatValue,
            ColumnType.Integer64 => value.LongValue,
            _ => throw new InvalidCastException()
        };
    }

    /// <summary>
    /// Returns the value of the specified column reconstructed as <typeparamref name="T"/>. Adds
    /// conversions the base <see cref="DbDataReader"/> cannot perform off the boxed CLR value —
    /// notably <see cref="DateOnly"/>, <see cref="TimeOnly"/>, <see cref="DateTimeOffset"/>,
    /// <see cref="byte"/>[] and <see cref="float"/> — so EF Core's typed materializers work.
    /// </summary>
    public override T GetFieldValue<T>(int ordinal)
    {
        Type target = typeof(T);

        if (target == typeof(DateOnly))
            return (T)(object)DateOnly.FromDateTime(GetDateTime(ordinal));

        if (target == typeof(DateTime))
            return (T)(object)GetDateTime(ordinal);

        if (target == typeof(DateTimeOffset))
            return (T)(object)new DateTimeOffset(DateTime.SpecifyKind(GetDateTime(ordinal), DateTimeKind.Utc));

        if (target == typeof(TimeOnly))
            return (T)(object)TimeOnly.FromDateTime(GetDateTime(ordinal));

        if (target == typeof(byte[]))
            return (T)(object)(GetColumnValue(ordinal).BytesValue ?? Array.Empty<byte>());

        // Typed arrays (long[], string[], double[], bool[], ...) from a native ARRAY column: the raw
        // value is an object?[]; project it into the requested element type.
        if (target.IsArray && GetColumnValue(ordinal) is { Type: ColumnType.Array } arrayColumn)
        {
            Type elementType = target.GetElementType()!;
            object?[] source = ConvertArray(in arrayColumn);
            Array typed = Array.CreateInstance(elementType, source.Length);

            for (int i = 0; i < source.Length; i++)
            {
                object? element = source[i];
                if (element is null)
                    typed.SetValue(null, i);
                else
                    typed.SetValue(
                        elementType.IsInstanceOfType(element)
                            ? element
                            : Convert.ChangeType(element, elementType, CultureInfo.InvariantCulture),
                        i);
            }

            return (T)(object)typed;
        }

        if (target == typeof(float))
            return (T)(object)GetFloat(ordinal);

        if (target == typeof(Guid))
            return (T)(object)GetGuid(ordinal);

        return base.GetFieldValue<T>(ordinal);
    }

    public override IEnumerator GetEnumerator()
    {
        while (Read())
            yield return this;
    }

    public override float GetFloat(int ordinal) => Convert.ToSingle(GetDouble(ordinal), CultureInfo.InvariantCulture);

    public override Guid GetGuid(int ordinal)
    {
        ColumnValue column = GetColumnValue(ordinal);

        if (column.Type == ColumnType.Uuid)
            return column.AsGuid();

        return Guid.Parse(GetString(ordinal));
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
            ColumnType.Uuid => value.UuidValue ?? value.AsGuid().ToString("D"),
            _ => Convert.ToString(GetValue(ordinal), CultureInfo.InvariantCulture) ?? ""
        };
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            Close();

        base.Dispose(disposing);
    }

    public ColumnValue GetColumnValue(int ordinal)
    {
        if (ordinal < 0 || ordinal >= columnNames.Length)
            throw new IndexOutOfRangeException();

        ThrowIfClosed();

        if (position < 0 || position >= resultSet.RowCount)
            throw new InvalidOperationException("No current row is available.");

        return resultSet.GetCell(position, ordinal);
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
