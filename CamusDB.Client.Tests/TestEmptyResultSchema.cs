/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;

namespace CamusDB.Client.Tests;

/// <summary>
/// Deterministic (server-free) coverage of the zero-row output-schema surface: when the server reports
/// an empty result together with a <c>columns</c> block, the reader must still expose the full schema
/// (field count, names and types) with no current row — the shape EF Core's buffered reader requires
/// under <c>EnableRetryOnFailure</c>.
/// </summary>
public class TestEmptyResultSchema
{
    [Fact]
    public void ColumnSchemaDeserializesNameAndType()
    {
        const string json = """{"name":"Id","type":1}""";

        CamusColumnSchema? schema = JsonSerializer.Deserialize(
            json, CamusJsonSerializerContext.Default.CamusColumnSchema);

        Assert.NotNull(schema);
        Assert.Equal("Id", schema!.Name);
        Assert.Equal(ColumnType.Id, schema.Type);
    }

    [Fact]
    public void EmptyWithSchemaReportsColumnsButNoRows()
    {
        CamusResultSet resultSet = CamusResultSet.EmptyWithSchema(
        [
            new CamusColumnSchema { Name = "Id", Type = ColumnType.Id },
            new CamusColumnSchema { Name = "Name", Type = ColumnType.String },
            new CamusColumnSchema { Name = "Year", Type = ColumnType.Integer64 },
        ]);

        Assert.Equal(3, resultSet.ColumnCount);
        Assert.Equal(0, resultSet.RowCount);
        Assert.Equal(["Id", "Name", "Year"], resultSet.ColumnNames);
        Assert.NotNull(resultSet.ColumnTypes);
    }

    [Fact]
    public void ReaderExposesSchemaWithoutAdvancingToARow()
    {
        // This is exactly what EF Core's BufferedDataReader.InitializeFields does before reading any
        // row: read FieldCount, then GetName/GetFieldType for each column. It must not require a
        // current row.
        CamusResultSet resultSet = CamusResultSet.EmptyWithSchema(
        [
            new CamusColumnSchema { Name = "Id", Type = ColumnType.Id },
            new CamusColumnSchema { Name = "Name", Type = ColumnType.String },
        ]);

        using CamusDataReader reader = new(resultSet);

        Assert.Equal(2, reader.FieldCount);
        Assert.False(reader.HasRows);

        Assert.Equal("Id", reader.GetName(0));
        Assert.Equal("Name", reader.GetName(1));

        Assert.Equal(typeof(string), reader.GetFieldType(0));
        Assert.Equal(typeof(string), reader.GetFieldType(1));

        Assert.Equal(1, reader.GetOrdinal("Name"));

        // No rows: the first Read returns false and no cell access is attempted.
        Assert.False(reader.Read());
    }

    [Fact]
    public void FromWireEmptyRowsStillCarriesSchema()
    {
        using JsonDocument rows = JsonDocument.Parse("[]");

        CamusResultSet resultSet = CamusResultSet.FromWire(
            [
                new CamusColumnSchema { Name = "Id", Type = ColumnType.Id },
                new CamusColumnSchema { Name = "Name", Type = ColumnType.String },
            ],
            rows.RootElement);

        Assert.Equal(2, resultSet.ColumnCount);
        Assert.Equal(0, resultSet.RowCount);

        using CamusDataReader reader = new(resultSet);
        Assert.Equal(2, reader.FieldCount);
        Assert.Equal("Id", reader.GetName(0));
        Assert.Equal(typeof(string), reader.GetFieldType(1));
    }

    [Fact]
    public void FromWireDecodesPositionalCellsByDeclaredType()
    {
        // rows[r][c] aligns to columns[c]; DateTime is ticks; a null cell is JSON null for any type.
        const long ticks = 638000000000000000L;
        string rowsJson = $"[[\"a1b2c3d4e5f6a7b8c9d0e1f2\",\"bolt\",42,true,3.5,{ticks}],[\"c3d4e5f6a7b8c9d0e1f2a3b4\",null,0,false,1.25,{ticks}]]";
        using JsonDocument rows = JsonDocument.Parse(rowsJson);

        CamusResultSet resultSet = CamusResultSet.FromWire(
            [
                new CamusColumnSchema { Name = "Id", Type = ColumnType.Id },
                new CamusColumnSchema { Name = "Name", Type = ColumnType.String },
                new CamusColumnSchema { Name = "Qty", Type = ColumnType.Integer64 },
                new CamusColumnSchema { Name = "Active", Type = ColumnType.Bool },
                new CamusColumnSchema { Name = "Ratio", Type = ColumnType.Float64 },
                new CamusColumnSchema { Name = "When", Type = ColumnType.DateTime },
            ],
            rows.RootElement);

        Assert.Equal(2, resultSet.RowCount);

        using CamusDataReader reader = new(resultSet);

        Assert.True(reader.Read());
        Assert.Equal("a1b2c3d4e5f6a7b8c9d0e1f2", reader.GetString(0));
        Assert.Equal("bolt", reader.GetString(1));
        Assert.Equal(42, reader.GetInt64(2));
        Assert.True(reader.GetBoolean(3));
        Assert.Equal(3.5, reader.GetDouble(4));
        Assert.Equal(new DateTime(ticks, DateTimeKind.Utc), reader.GetDateTime(5));

        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(1));
        Assert.False(reader.GetBoolean(3));
        Assert.Equal(1.25, reader.GetDouble(4));

        Assert.False(reader.Read());
    }

    [Fact]
    public void ReaderFieldTypeMapsDeclaredSchemaTypes()
    {
        CamusResultSet resultSet = CamusResultSet.EmptyWithSchema(
        [
            new CamusColumnSchema { Name = "Flag", Type = ColumnType.Bool },
            new CamusColumnSchema { Name = "Amount", Type = ColumnType.Float64 },
            new CamusColumnSchema { Name = "Count", Type = ColumnType.Integer64 },
            new CamusColumnSchema { Name = "When", Type = ColumnType.DateTime },
        ]);

        using CamusDataReader reader = new(resultSet);

        Assert.Equal(typeof(bool), reader.GetFieldType(0));
        Assert.Equal(typeof(double), reader.GetFieldType(1));
        Assert.Equal(typeof(long), reader.GetFieldType(2));
        Assert.Equal(typeof(DateTime), reader.GetFieldType(3));
    }
}
