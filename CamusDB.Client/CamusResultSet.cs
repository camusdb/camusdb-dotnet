
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace CamusDB.Client;

/// <summary>
/// Columnar, positional storage for a query result. Rows are held in a single flat
/// <see cref="ColumnValue"/>[] (row-major: cell <c>(row, col)</c> lives at <c>row * ColumnCount + col</c>)
/// with the column names captured once from the first row.
///
/// <para>This replaces the previous <c>List&lt;Dictionary&lt;string, ColumnValue&gt;&gt;</c> shape: it
/// removes one <see cref="System.Collections.Generic.Dictionary{TKey,TValue}"/> allocation per row and
/// turns every per-cell access in <see cref="CamusDataReader"/> from a string-hashed dictionary lookup
/// into a direct array index.</para>
/// </summary>
[JsonConverter(typeof(CamusResultSetConverter))]
public sealed class CamusResultSet
{
    /// <summary>An empty result (zero rows, zero columns), shared to avoid allocating for count-only readers.</summary>
    public static readonly CamusResultSet Empty = new([], [], 0);

    private readonly ColumnValue[] cells;

    public string[] ColumnNames { get; }

    public int RowCount { get; }

    public int ColumnCount => ColumnNames.Length;

    public CamusResultSet(string[] columnNames, ColumnValue[] cells, int rowCount)
    {
        ColumnNames = columnNames;
        this.cells = cells;
        RowCount = rowCount;
    }

    public ColumnValue GetCell(int row, int column) => cells[row * ColumnCount + column];

    /// <summary>
    /// Builds a result set from a list of row dictionaries (column name → value). The column schema is
    /// taken from the first row. Provided for callers that materialize rows by name — the wire path uses
    /// <see cref="CamusResultSetConverter"/> to populate the flat storage directly.
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
}

/// <summary>
/// Reads the server's <c>"rows"</c> payload — a JSON array of row objects keyed by column name — directly
/// into <see cref="CamusResultSet"/>'s flat backing array, without materializing a dictionary per row.
/// The column schema is established from the first row (a SQL result set has the same columns in every
/// row); later rows are placed by resolved ordinal, so a server that reordered a row's keys still maps
/// correctly.
/// </summary>
internal sealed class CamusResultSetConverter : JsonConverter<CamusResultSet>
{
    public override CamusResultSet? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
            return null;

        if (reader.TokenType != JsonTokenType.StartArray)
            throw new JsonException("Expected the start of the rows array.");

        JsonTypeInfo<ColumnValue> columnInfo = CamusJsonSerializerContext.Default.ColumnValue;

        List<string> names = [];
        Dictionary<string, int> ordinals = new(StringComparer.Ordinal);
        List<ColumnValue> cells = [];
        int columnCount = -1;
        int rowCount = 0;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndArray)
                break;

            if (reader.TokenType != JsonTokenType.StartObject)
                throw new JsonException("Expected a row object.");

            if (columnCount < 0)
            {
                // First row: encounter order defines the ordinals; append each cell as discovered.
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    string name = reader.GetString()!;
                    reader.Read();
                    ColumnValue value = JsonSerializer.Deserialize(ref reader, columnInfo);

                    ordinals[name] = names.Count;
                    names.Add(name);
                    cells.Add(value);
                }

                columnCount = names.Count;
            }
            else
            {
                // Reserve this row's slots so out-of-order properties still land at the right ordinal.
                int rowBase = cells.Count;
                for (int i = 0; i < columnCount; i++)
                    cells.Add(ColumnValue.Null);

                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    string name = reader.GetString()!;
                    reader.Read();
                    ColumnValue value = JsonSerializer.Deserialize(ref reader, columnInfo);

                    if (ordinals.TryGetValue(name, out int ordinal))
                        cells[rowBase + ordinal] = value;
                }
            }

            rowCount++;
        }

        if (rowCount == 0)
            return CamusResultSet.Empty;

        return new CamusResultSet([.. names], [.. cells], rowCount);
    }

    public override void Write(Utf8JsonWriter writer, CamusResultSet value, JsonSerializerOptions options)
        => throw new NotSupportedException("CamusResultSet is a read-only response type and is never serialized.");
}
