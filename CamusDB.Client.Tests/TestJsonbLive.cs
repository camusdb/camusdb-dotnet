/**
 * This file is part of CamusDB
 *
 * Live coverage for the JSONB type and the GIN index over REST (buffered and streamed) and gRPC, and
 * through EF Core. Needs a server on :5095 (REST) and :5096 (gRPC) that supports JSONB.
 */

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

public class TestJsonbLive : BaseTest
{
    private static CamusConnectionStringBuilder BuilderFor(string protocol) => protocol == "grpc"
        ? new("Endpoint=http://localhost:5096;Database=test;Protocol=grpc")
        : new("Endpoint=http://localhost:5095;Database=test");

    private static async Task<CamusConnection> OpenAsync(string protocol)
    {
        CamusConnection connection = new(BuilderFor(protocol));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync(ifNotExists: true);
        return connection;
    }

    private static Task<string> CreateEventsAsync(CamusConnection connection, string prefix)
        => CreateTempTableAsync(connection, prefix, " id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, doc JSONB");

    private static async Task InsertAsync(CamusConnection connection, string table, string name, object? doc, ColumnType type = ColumnType.Null)
    {
        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, doc) VALUES (@id, @name, @doc)");

        insert.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
        insert.Parameters.Add("@name", ColumnType.String, name);
        insert.Parameters.Add("@doc", type, doc ?? DBNull.Value);

        Assert.Equal(1, await insert.ExecuteNonQueryAsync());
    }

    private static async Task<List<string>> NamesAsync(CamusConnection connection, string sql, params (string Name, ColumnType Type, object Value)[] parameters)
    {
        await using CamusCommand select = connection.CreateCamusCommand(sql);

        foreach ((string name, ColumnType type, object value) in parameters)
            select.Parameters.Add(name, type, value);

        List<string> names = [];
        await using CamusDataReader reader = await select.ExecuteReaderAsync();

        while (await reader.ReadAsync())
            names.Add(reader.GetString(0));

        names.Sort(StringComparer.Ordinal);
        return names;
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task DocumentsRoundTripInTheTextForm(string protocol)
    {
        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateEventsAsync(connection, "jsonb");

        await InsertAsync(connection, table, "document", JsonDocument.Parse("""{"b":1,   "a":[1,2.50], "n": 12345678901234567890.50}"""));
        await InsertAsync(connection, table, "node", new JsonObject { ["kind"] = "login", ["tags"] = new JsonArray("vip") });
        await InsertAsync(connection, table, "text", """["x", {"y": null}]""", ColumnType.Jsonb);
        await InsertAsync(connection, table, "string", """{"plain": "string"}""", ColumnType.String);
        await InsertAsync(connection, table, "json-null", "null", ColumnType.Jsonb);
        await InsertAsync(connection, table, "sql-null", null);

        Dictionary<string, string?> rows = [];

        await using (CamusCommand select = connection.CreateCamusCommand($"SELECT name, doc FROM {table}"))
        await using (CamusDataReader reader = await select.ExecuteReaderAsync())
        {
            Assert.Equal(typeof(string), reader.GetFieldType(1));

            while (await reader.ReadAsync())
            {
                rows[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);

                if (reader.GetString(0) == "document")
                {
                    using JsonDocument document = reader.GetFieldValue<JsonDocument>(1);
                    Assert.Equal(12345678901234567890.50m, document.RootElement.GetProperty("n").GetDecimal());
                }
            }
        }

        Assert.Equal("""{"a": [1, 2.50], "b": 1, "n": 12345678901234567890.50}""", rows["document"]);
        Assert.Equal("""{"kind": "login", "tags": ["vip"]}""", rows["node"]);
        Assert.Equal("""["x", {"y": null}]""", rows["text"]);
        Assert.Equal("""{"plain": "string"}""", rows["string"]);
        Assert.Null(rows["sql-null"]);

        // Both transports tell the JSON null document from SQL NULL. Over REST the driver asks for the
        // text form (jsonbAsText), because the default REST row writes both as the token null.
        Assert.Equal("null", rows["json-null"]);

        // A text operand compares with a document as a document.
        Assert.Equal(["node"], await NamesAsync(connection,
            $"SELECT name FROM {table} WHERE doc @> @filter",
            ("@filter", ColumnType.Null, JsonNode.Parse("""{"kind": "login"}""")!)));

        Assert.Equal(["node"], await NamesAsync(connection,
            $"SELECT name FROM {table} WHERE doc ->> 'kind' = @kind",
            ("@kind", ColumnType.String, "login")));
    }

    [Fact]
    public async Task StreamedReaderDecodesJsonb()
    {
        await using CamusConnection connection = await OpenAsync("");
        string table = await CreateEventsAsync(connection, "jsonb_stream");

        await InsertAsync(connection, table, "deep", JsonDocument.Parse(new string('[', 300) + new string(']', 300), new JsonDocumentOptions { MaxDepth = 512 }));
        await InsertAsync(connection, table, "flat", """{"z": "é\n"}""", ColumnType.Jsonb);
        await InsertAsync(connection, table, "json-null", "null", ColumnType.Jsonb);
        await InsertAsync(connection, table, "string", "\"abc\"", ColumnType.Jsonb);
        await InsertAsync(connection, table, "sql-null", null);

        Dictionary<string, string?> rows = [];

        await using CamusCommand select = connection.CreateCamusCommand($"SELECT name, doc FROM {table}");
        await using CamusDataReader reader = await select.ExecuteStreamReaderAsync();

        while (await reader.ReadAsync())
        {
            if (reader.IsDBNull(1))
            {
                rows[reader.GetString(0)] = null;
                continue;
            }

            Assert.Equal(ColumnType.Jsonb, reader.GetColumnValue(1).Type);
            rows[reader.GetString(0)] = reader.GetString(1);
        }

        Assert.Equal(new string('[', 300) + new string(']', 300), rows["deep"]);
        Assert.Equal("{\"z\": \"é\\n\"}", rows["flat"]);
        Assert.Equal("null", rows["json-null"]);
        Assert.Equal("\"abc\"", rows["string"]);
        Assert.Null(rows["sql-null"]);
    }

    /// <summary>
    /// The RETURNING rows of a write over REST keep a JSON null document apart from SQL NULL too.
    /// </summary>
    [Fact]
    public async Task ReturningRowsKeepJsonNullOverRest()
    {
        await using CamusConnection connection = await OpenAsync("");
        string table = await CreateEventsAsync(connection, "jsonb_returning");

        await using CamusCommand insert = connection.CreateCamusCommand(
            $"INSERT INTO {table} (id, name, doc) VALUES (@a, 'json-null', @doc), (@b, 'sql-null', NULL) RETURNING name, doc");
        insert.Parameters.Add("@a", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
        insert.Parameters.Add("@b", ColumnType.Id, CamusObjectIdGenerator.GenerateAsString());
        insert.Parameters.Add("@doc", ColumnType.Jsonb, "null");

        Dictionary<string, string?> rows = [];

        await using CamusDataReader reader = await insert.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            rows[reader.GetString(0)] = reader.IsDBNull(1) ? null : reader.GetString(1);

        Assert.Equal("null", rows["json-null"]);
        Assert.Null(rows["sql-null"]);
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task InvalidJsonIsRefused(string protocol)
    {
        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateEventsAsync(connection, "jsonb_invalid");

        CamusException ex = await Assert.ThrowsAsync<CamusException>(
            () => InsertAsync(connection, table, "bad", """{"a": 1,}""", ColumnType.Jsonb));

        Assert.Equal("CADB0418", ex.Code);
    }

    [Theory]
    [InlineData("")]
    [InlineData("grpc")]
    public async Task GinIndexServesContainmentAndKeyQueries(string protocol)
    {
        await using CamusConnection connection = await OpenAsync(protocol);
        string table = await CreateEventsAsync(connection, "jsonb_gin");

        await using (CamusCommand index = connection.CreateCamusCommand($"CREATE INDEX {table}_gin ON {table} USING GIN (doc)"))
            Assert.True(await index.ExecuteDDLAsync());

        await InsertAsync(connection, table, "login", JsonNode.Parse("""{"kind": "login", "user": {"id": 7}}"""));
        await InsertAsync(connection, table, "logout", JsonNode.Parse("""{"kind": "logout", "user": {"id": 7}}"""));
        await InsertAsync(connection, table, "other", JsonNode.Parse("""{"kind": "login", "user": {"id": 8}, "beta": true}"""));

        Assert.Equal(["login"], await NamesAsync(connection,
            $"SELECT name FROM {table} WHERE jsonb_contains(doc, @filter)",
            ("@filter", ColumnType.Jsonb, """{"kind": "login", "user": {"id": 7}}""")));

        Assert.Equal(["other"], await NamesAsync(connection,
            $"SELECT name FROM {table} WHERE jsonb_exists(doc, @key)", ("@key", ColumnType.String, "beta")));

        Assert.Equal(["login", "logout"], await NamesAsync(connection,
            $"SELECT name FROM {table} WHERE doc @> @filter", ("@filter", ColumnType.Jsonb, """{"user": {"id": 7}}""")));

        // The planner reads the GIN index for the containment conjunct.
        StringBuilder plan = new();

        await using (CamusCommand explain = connection.CreateCamusCommand(
            $"EXPLAIN SELECT name FROM {table} WHERE doc @> '{{\"kind\": \"login\"}}'"))
        await using (CamusDataReader reader = await explain.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                    if (!reader.IsDBNull(i))
                        plan.AppendLine(reader.GetValue(i).ToString());
            }
        }

        Assert.Contains("gin-scan", plan.ToString());
    }

    [Fact]
    public async Task EntityFrameworkSavesAndFiltersDocuments()
    {
        DbContextOptions<EventContext> options = new DbContextOptionsBuilder<EventContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test").Options;

        await using (EventContext ctx = new(options))
            await ctx.Database.EnsureCreatedAsync();

        // EnsureCreated creates no index of the model, so the GIN index comes from its own statement.
        await using (EventContext ctx = new(options))
            await ctx.Database.ExecuteSqlRawAsync("CREATE INDEX IF NOT EXISTS ef_jsonb_events_gin ON ef_jsonb_events USING GIN (Payload)");

        string tag = Guid.NewGuid().ToString("n");

        await using (EventContext ctx = new(options))
        {
            ctx.Events.Add(new EventRow { Id = CamusObjectIdGenerator.GenerateAsString(), Tag = tag, Payload = JsonDocument.Parse("""{"kind": "login", "n": 1.50}"""), Meta = new JsonObject { ["seen"] = false } });
            ctx.Events.Add(new EventRow { Id = CamusObjectIdGenerator.GenerateAsString(), Tag = tag, Payload = JsonDocument.Parse("""{"kind": "logout"}"""), Text = """{"a": 1}""" });
            await ctx.SaveChangesAsync();
        }

        await using (EventContext ctx = new(options))
        {
            EventRow login = await ctx.Events.SingleAsync(e => e.Tag == tag && EF.Functions.JsonContains(e.Payload, """{"kind": "login"}"""));

            Assert.Equal("1.50", login.Payload.RootElement.GetProperty("n").GetRawText());
            Assert.False(login.Meta!["seen"]!.GetValue<bool>());

            // A change inside the tracked JsonNode is saved.
            login.Meta["seen"] = true;
            Assert.Equal(1, await ctx.SaveChangesAsync());

            JsonDocument filter = JsonDocument.Parse("""{"kind": "logout"}""");
            EventRow logout = await ctx.Events.AsNoTracking().SingleAsync(e => e.Tag == tag && EF.Functions.JsonContains(e.Payload, filter));
            Assert.Equal("""{"a": 1}""", logout.Text);

            Assert.Equal(1, await ctx.Events.CountAsync(e => e.Tag == tag && EF.Functions.JsonExists(e.Text!, "a")));
            Assert.Equal(2, await ctx.Events.CountAsync(e => e.Tag == tag && EF.Functions.JsonTypeof(e.Payload) == "object"));
        }

        await using (EventContext ctx = new(options))
        {
            EventRow login = await ctx.Events.AsNoTracking().SingleAsync(e => e.Tag == tag && EF.Functions.JsonExists(e.Payload, "n"));
            Assert.True(login.Meta!["seen"]!.GetValue<bool>());
        }
    }

    public class EventRow
    {
        public string Id { get; set; } = "";

        public string Tag { get; set; } = "";

        public JsonDocument Payload { get; set; } = null!;

        public JsonObject? Meta { get; set; }

        public string? Text { get; set; }
    }

    private class EventContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<EventRow> Events => Set<EventRow>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<EventRow>(b =>
            {
                b.ToTable("ef_jsonb_events");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Tag).HasMaxLength(32);
                b.Property(e => e.Text).HasColumnType("jsonb");
                b.HasIndex(e => e.Payload).HasMethod("gin");
            });
        }
    }
}
