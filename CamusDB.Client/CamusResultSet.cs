
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;

namespace CamusDB.Client;

/// <summary>
/// Columnar, positional storage for a query result. Rows are held in a single flat
/// <see cref="ColumnValue"/>[] (row-major: cell <c>(row, col)</c> lives at <c>row * ColumnCount + col</c>).
/// The column names and declared types come from the response's authoritative <c>columns</c> schema, so
/// the shape is known even when there are no rows.
///
/// <para>The wire format is an ordered <c>columns</c> array plus positional <c>rows</c> (each row a JSON
/// array aligned to <c>columns</c>). <see cref="FromWire"/> decodes each positional cell against its
/// declared <see cref="ColumnType"/>.</para>
/// </summary>
public sealed class CamusResultSet
{
    /// <summary>An empty result (zero rows, zero columns), shared to avoid allocating for count-only readers.</summary>
    public static readonly CamusResultSet Empty = new([], [], 0);

    private readonly ColumnValue[] cells;

    public string[] ColumnNames { get; }

    /// <summary>
    /// Declared type of each column, positionally aligned with <see cref="ColumnNames"/>, when the
    /// server supplied an explicit output schema. Null only for results materialized by name via
    /// <see cref="FromRows"/> (test helpers), in which case field types are read from the current row.
    /// </summary>
    public ColumnType[]? ColumnTypes { get; }

    public int RowCount { get; }

    public int ColumnCount => ColumnNames.Length;

    public CamusResultSet(string[] columnNames, ColumnValue[] cells, int rowCount)
        : this(columnNames, cells, rowCount, columnTypes: null)
    {
    }

    public CamusResultSet(string[] columnNames, ColumnValue[] cells, int rowCount, ColumnType[]? columnTypes)
    {
        ColumnNames = columnNames;
        this.cells = cells;
        RowCount = rowCount;
        ColumnTypes = columnTypes;
    }

    /// <summary>
    /// Builds a result from the server's authoritative <c>columns</c> schema and positional <c>rows</c>
    /// payload. Each <c>rows[r][c]</c> value is decoded against <c>columns[c]</c>'s declared type. Works
    /// for zero rows (the schema still yields the full column list), which is what lets the reader report
    /// field count / names / types before the first row is read.
    /// </summary>
    public static CamusResultSet FromWire(IReadOnlyList<CamusColumnSchema> columns, JsonElement rows)
    {
        int columnCount = columns.Count;
        string[] names = new string[columnCount];
        ColumnType[] types = new ColumnType[columnCount];

        for (int i = 0; i < columnCount; i++)
        {
            names[i] = columns[i].Name;
            types[i] = columns[i].Type;
        }

        if (rows.ValueKind != JsonValueKind.Array || columnCount == 0)
            return new CamusResultSet(names, [], 0, types);

        int rowCount = rows.GetArrayLength();
        ColumnValue[] cells = new ColumnValue[rowCount * columnCount];

        int r = 0;
        foreach (JsonElement row in rows.EnumerateArray())
        {
            int rowBase = r * columnCount;
            int c = 0;

            foreach (JsonElement cell in row.EnumerateArray())
            {
                if (c >= columnCount)
                    break;

                cells[rowBase + c] = DecodeCell(cell, types[c]);
                c++;
            }

            // Any positions the row omitted stay ColumnValue.Null (the array default).
            r++;
        }

        return new CamusResultSet(names, cells, rowCount, types);
    }

    public ColumnValue GetCell(int row, int column) => cells[row * ColumnCount + column];

    /// <summary>
    /// Builds a zero-row result that still carries the query's output schema (column names + declared
    /// types). Retained for callers/tests that construct a schema-only reader directly.
    /// </summary>
    public static CamusResultSet EmptyWithSchema(IReadOnlyList<CamusColumnSchema> columns)
    {
        string[] names = new string[columns.Count];
        ColumnType[] types = new ColumnType[columns.Count];

        for (int i = 0; i < columns.Count; i++)
        {
            names[i] = columns[i].Name;
            types[i] = columns[i].Type;
        }

        return new CamusResultSet(names, [], 0, types);
    }

    /// <summary>
    /// Builds a result set from a list of row dictionaries (column name → value). The column schema is
    /// taken from the first row. Provided for callers/tests that materialize rows by name; the wire path
    /// uses <see cref="FromWire"/>.
    /// </summary>
    public static CamusResultSet FromRows(IReadOnlyList<IReadOnlyDictionary<string, ColumnValue>> rows)
    {
        if (rows.Count == 0)
            return Empty;

        string[] columnNames = [.. rows[0].Keys];
        ColumnValue[] cells = new ColumnValue[rows.Count * columnNames.Length];

        for (int r = 0; r < rows.Count; r++)
        {
            IReadOnlyDictionary<string, ColumnValue> row = rows[r];
            int rowBase = r * columnNames.Length;

            for (int c = 0; c < columnNames.Length; c++)
                cells[rowBase + c] = row.TryGetValue(columnNames[c], out ColumnValue value) ? value : ColumnValue.Null;
        }

        return new CamusResultSet(columnNames, cells, rows.Count);
    }

    /// <summary>
    /// Decodes one compact-raw positional cell into a <see cref="ColumnValue"/> using its declared type.
    /// A JSON <c>null</c> is <see cref="ColumnValue.Null"/> for any column type. String/array-encoded
    /// types (Id, Bytes, Date, DateTime, Uuid, Array) rely on the declared type being exact — it always
    /// is for real column references. JSON-native scalars are read by token so an over-broad inferred
    /// type on an expression column (e.g. a numeric projection reported as String) still round-trips.
    /// </summary>
    private static ColumnValue DecodeCell(JsonElement cell, ColumnType declared)
    {
        if (cell.ValueKind == JsonValueKind.Null || cell.ValueKind == JsonValueKind.Undefined)
            return ColumnValue.Null;

        switch (declared)
        {
            case ColumnType.Id:
                return new ColumnValue { Type = ColumnType.Id, StrValue = cell.GetString() };

            case ColumnType.Bytes:
                return new ColumnValue { Type = ColumnType.Bytes, BytesValue = cell.GetBytesFromBase64() };

            case ColumnType.Date:
                return new ColumnValue { Type = ColumnType.Date, LongValue = cell.GetInt64() };

            case ColumnType.DateTime:
                return new ColumnValue { Type = ColumnType.DateTime, LongValue = cell.GetInt64() };

            case ColumnType.Uuid:
                return DecodeUuid(cell);

            case ColumnType.Array:
                return DecodeArray(cell);

            default:
                // Defensive recovery for a server type-inference gap: JOIN projections can report a
                // `uuid` column's type as String (3) while still sending the `[high, low]` two-int64
                // wire form. A scalar cell never legitimately arrives as a JSON array, so a 2-element
                // array here is that mis-tagged UUID — decode it structurally instead of trusting the
                // declared type and dropping the value to Null.
                if (cell.ValueKind == JsonValueKind.Array && cell.GetArrayLength() == 2)
                    return DecodeUuid(cell);

                return DecodeScalarByToken(cell, declared);
        }
    }

    // Uuid is wired as [high, low] — two big-endian 64-bit halves (see ColumnValue.AsGuid). A canonical
    // string form is accepted as a fallback.
    private static ColumnValue DecodeUuid(JsonElement cell)
    {
        if (cell.ValueKind == JsonValueKind.Array && cell.GetArrayLength() == 2)
            return new ColumnValue
            {
                Type = ColumnType.Uuid,
                UuidHigh = cell[0].GetInt64(),
                LongValue = cell[1].GetInt64(),
            };

        if (cell.ValueKind == JsonValueKind.String)
            return new ColumnValue { Type = ColumnType.Uuid, UuidValue = cell.GetString() };

        return ColumnValue.Null;
    }

    // The wire schema carries no per-element type for arrays, so element types are inferred from the JSON
    // token. Fully faithful for JSON-native element types; string/array-encoded element types (uuid/date/
    // bytes inside an array) are not reconstructed — arrays are not part of the EF workload.
    private static ColumnValue DecodeArray(JsonElement cell)
    {
        if (cell.ValueKind != JsonValueKind.Array)
            return ColumnValue.Null;

        List<ColumnValue> values = new(cell.GetArrayLength());
        ColumnType elementType = ColumnType.Null;

        foreach (JsonElement item in cell.EnumerateArray())
        {
            ColumnValue decoded = DecodeScalarByToken(item, ColumnType.Null);
            if (decoded.Type != ColumnType.Null)
                elementType = decoded.Type;
            values.Add(decoded);
        }

        return new ColumnValue { Type = ColumnType.Array, ArrayValues = values, ArrayElementType = elementType };
    }

    private static ColumnValue DecodeScalarByToken(JsonElement cell, ColumnType declared)
    {
        switch (cell.ValueKind)
        {
            case JsonValueKind.String:
                return new ColumnValue { Type = ColumnType.String, StrValue = cell.GetString() };

            case JsonValueKind.True:
            case JsonValueKind.False:
                return new ColumnValue { Type = ColumnType.Bool, BoolValue = cell.GetBoolean() };

            case JsonValueKind.Number:
                if (declared is ColumnType.Float64 or ColumnType.Float32)
                    return new ColumnValue { Type = declared, FloatValue = cell.GetDouble() };

                if (cell.TryGetInt64(out long l))
                    return new ColumnValue { Type = ColumnType.Integer64, LongValue = l };

                return new ColumnValue { Type = ColumnType.Float64, FloatValue = cell.GetDouble() };

            default:
                return ColumnValue.Null;
        }
    }
}
