


using System.Collections;
using System.Data.Common;
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

public class CamusDataReader : DbDataReader
{
	private int position;

	private readonly List<Dictionary<string, ColumnValue>> rows;

    public override int Depth => throw new NotImplementedException();

    public override int FieldCount => throw new NotImplementedException();

    public override bool HasRows => throw new NotImplementedException();

    public override bool IsClosed => throw new NotImplementedException();

    public override int RecordsAffected => throw new NotImplementedException();

    public override object this[string name] => throw new NotImplementedException();

    public override object this[int ordinal] => throw new NotImplementedException();

    public CamusDataReader(List<Dictionary<string, ColumnValue>> rows)
	{
		this.position = 0;
		this.rows = rows;
	}

    /// <summary>
    /// Reads the next row of values from Cloud Spanner.
    /// Important: Cloud Spanner supports limited cancellation of this task.
    /// </summary>
    /// <param name="cancellationToken">A cancellation token to cancel the read. Cloud Spanner currently
    /// supports limited cancellation while advancing the read to the next row.</param>
    /// <returns>True if another row was read.</returns>
    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
	{
        position++;

        if (position >= rows.Count)
			return Task.FromResult(false);
		
		return Task.FromResult(true);
	}

	private Dictionary<string, ColumnValue> GetCurrent()
	{
		return rows[position];
	}

	public override bool IsDBNull(int ordinal)
	{
        Dictionary<string, ColumnValue> current = GetCurrent();

        int i = 0;

        foreach (KeyValuePair<string, ColumnValue> keyValue in current)
        {
            if (i == ordinal)
                return keyValue.Value.Type == ColumnType.Null;

            i++;
        }

        return false;
    }


    public override int GetInt32(int ordinal)
	{
        Dictionary<string, ColumnValue> current = GetCurrent();

		int i = 0;

		foreach (KeyValuePair<string, ColumnValue> keyValue in current)
		{
			if (i == ordinal)
				return (int)keyValue.Value.LongValue;

			i++;
		}

		return 0;
	}

    public override string GetString(int ordinal)
    {
        Dictionary<string, ColumnValue> current = GetCurrent();

        int i = 0;

        foreach (KeyValuePair<string, ColumnValue> keyValue in current)
        {
            if (i == ordinal)
                return keyValue.Value.StrValue!;

            i++;
        }

        return null!;
    }

    public override bool GetBoolean(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override byte GetByte(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override char GetChar(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override DateTime GetDateTime(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override decimal GetDecimal(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override double GetDouble(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

    public override Type GetFieldType(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override float GetFloat(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override Guid GetGuid(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override short GetInt16(int ordinal)
    {
        Dictionary<string, ColumnValue> current = GetCurrent();

        int i = 0;

        foreach (KeyValuePair<string, ColumnValue> keyValue in current)
        {
            if (i == ordinal)
                return (short)keyValue.Value.LongValue;

            i++;
        }

        return 0;
    }    

    public override long GetInt64(int ordinal)
    {
        Dictionary<string, ColumnValue> current = GetCurrent();

        int i = 0;

        foreach (KeyValuePair<string, ColumnValue> keyValue in current)
        {
            if (i == ordinal)
                return keyValue.Value.LongValue;

            i++;
        }

        return 0;
    }

    public override string GetName(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetOrdinal(string name)
    {
        throw new NotImplementedException();
    }    

    public override object GetValue(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override int GetValues(object[] values)
    {
        throw new NotImplementedException();
    }    

    public override bool NextResult()
    {
        throw new NotImplementedException();
    }

    public override bool Read()
    {
        throw new NotImplementedException();
    }
}

