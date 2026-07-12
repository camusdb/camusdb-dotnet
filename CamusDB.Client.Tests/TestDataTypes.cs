
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;

namespace CamusDB.Client.Tests;

public class TestDataTypes : BaseTest
{
    private static readonly SemaphoreSlim schemaLock = new(1, 1);

    private static async Task<string> CreateTypesTableAsync(CamusConnection connection)
    {
        await schemaLock.WaitAsync();
        try
        {
            await connection.CreateDatabaseAsync(ifNotExists: true);

            for (int attempt = 0; attempt < 5; attempt++)
            {
                string tableName = "types_" + Guid.NewGuid().ToString("n");
                string sql =
                    $"CREATE TABLE {tableName} (" +
                    " id OID PRIMARY KEY NOT NULL," +
                    " ref UUID," +
                    " name STRING(64)," +
                    " payload BYTES," +
                    " score FLOAT32," +
                    " happened DATETIME," +
                    " day DATE," +
                    " tags ARRAY(INT64))";

                try
                {
                    await using CamusCommand cmd = connection.CreateCamusCommand(sql);
                    Assert.True(await cmd.ExecuteDDLAsync());
                    return tableName;
                }
                catch (CamusException ex) when (
                    ex.Message.Contains("AlreadyLocked", StringComparison.Ordinal) ||
                    ex.Message.Contains("MustRetry", StringComparison.Ordinal) ||
                    ex.Message.Contains("commit returned Aborted", StringComparison.Ordinal) ||
                    ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase))
                {
                    if (attempt == 4)
                        throw;
                    await Task.Delay(50 * (attempt + 1));
                }
            }
        }
        finally
        {
            schemaLock.Release();
        }

        throw new InvalidOperationException("Unreachable.");
    }

    [Fact]
    public async Task TestRoundTripScalarTypes()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTypesTableAsync(connection);

        string id = CamusObjectIdGenerator.GenerateAsString();
        byte[] payload = [0xDE, 0xAD, 0xBE, 0xEF];
        float score = 3.5f;
        DateTime happened = new(2026, 3, 15, 12, 30, 0, DateTimeKind.Utc);
        DateOnly day = new(2026, 3, 15);

        await using (CamusCommand insert = connection.CreateInsertCommand(tableName))
        {
            insert.Parameters.Add("id", ColumnType.Id, id);
            insert.Parameters.Add("name", ColumnType.String, "events");
            insert.Parameters.Add("payload", ColumnType.Bytes, payload);
            insert.Parameters.Add("score", ColumnType.Float32, score);
            insert.Parameters.Add("happened", ColumnType.DateTime, happened);
            insert.Parameters.Add("day", ColumnType.Date, day);
            insert.Parameters.Add("tags", ColumnType.Integer64, new long[] { 1, 2, 3 }, isArray: true);

            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await using CamusCommand select = connection.CreateCamusCommand(
            $"SELECT id, name, payload, score, happened, day, tags FROM {tableName} WHERE id = @id");
        select.Parameters.Add("@id", ColumnType.Id, id);

        await using CamusDataReader reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(default));

        Assert.Equal(id, reader.GetString(reader.GetOrdinal("id")));
        Assert.Equal("events", reader.GetString(reader.GetOrdinal("name")));
        Assert.Equal(payload, reader.GetFieldValue<byte[]>(reader.GetOrdinal("payload")));
        Assert.Equal(score, reader.GetFloat(reader.GetOrdinal("score")));
        Assert.Equal(happened, reader.GetDateTime(reader.GetOrdinal("happened")));
        Assert.Equal(day, reader.GetFieldValue<DateOnly>(reader.GetOrdinal("day")));

        object?[] tags = (object?[])reader.GetValue(reader.GetOrdinal("tags"));
        Assert.Equal(new object?[] { 1L, 2L, 3L }, tags);
    }

    [Fact]
    public async Task TestRoundTripUuid()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTypesTableAsync(connection);

        string id = CamusObjectIdGenerator.GenerateAsString();
        Guid reference = Guid.NewGuid();

        await using (CamusCommand insert = connection.CreateInsertCommand(tableName))
        {
            insert.Parameters.Add("id", ColumnType.Id, id);
            insert.Parameters.Add("ref", ColumnType.Uuid, reference);
            insert.Parameters.Add("name", ColumnType.String, "uuid");

            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        // Read back through GetGuid, the typed materializer, and filter by the UUID value.
        await using CamusCommand select = connection.CreateCamusCommand(
            $"SELECT id, ref FROM {tableName} WHERE ref = @ref");
        select.Parameters.Add("@ref", ColumnType.Uuid, reference);

        await using CamusDataReader reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(default));

        int refOrdinal = reader.GetOrdinal("ref");
        Assert.Equal(ColumnType.Uuid, reader.GetColumnValue(refOrdinal).Type);
        Assert.Equal(reference, reader.GetGuid(refOrdinal));
        Assert.Equal(reference, reader.GetFieldValue<Guid>(refOrdinal));
        Assert.Equal(reference.ToString("D"), reader.GetString(refOrdinal));
    }

    [Fact]
    public async Task TestNullableUuid()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTypesTableAsync(connection);

        string id = CamusObjectIdGenerator.GenerateAsString();

        await using (CamusCommand insert = connection.CreateInsertCommand(tableName))
        {
            insert.Parameters.Add("id", ColumnType.Id, id);
            insert.Parameters.Add("ref", ColumnType.Null, null);
            insert.Parameters.Add("name", ColumnType.String, "n");

            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await using CamusCommand select = connection.CreateCamusCommand(
            $"SELECT ref FROM {tableName} WHERE id = @id");
        select.Parameters.Add("@id", ColumnType.Id, id);

        await using CamusDataReader reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(default));

        Assert.True(reader.IsDBNull(reader.GetOrdinal("ref")));
    }

    [Fact]
    public async Task TestNullableNewTypes()
    {
        CamusConnection connection = await GetConnection();
        string tableName = await CreateTypesTableAsync(connection);

        string id = CamusObjectIdGenerator.GenerateAsString();

        await using (CamusCommand insert = connection.CreateInsertCommand(tableName))
        {
            insert.Parameters.Add("id", ColumnType.Id, id);
            insert.Parameters.Add("name", ColumnType.String, "n");
            insert.Parameters.Add("payload", ColumnType.Null, null);
            insert.Parameters.Add("score", ColumnType.Null, null);
            insert.Parameters.Add("happened", ColumnType.Null, null);
            insert.Parameters.Add("day", ColumnType.Null, null);
            insert.Parameters.Add("tags", ColumnType.Null, null);

            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await using CamusCommand select = connection.CreateCamusCommand(
            $"SELECT payload, score, happened, day FROM {tableName} WHERE id = @id");
        select.Parameters.Add("@id", ColumnType.Id, id);

        await using CamusDataReader reader = await select.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(default));

        Assert.True(reader.IsDBNull(reader.GetOrdinal("payload")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("score")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("happened")));
        Assert.True(reader.IsDBNull(reader.GetOrdinal("day")));
    }
}
