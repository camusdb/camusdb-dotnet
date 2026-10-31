/**
 * This file is part of CamusDB
 *
 * Offline coverage for the JSONB type (ColumnType.Jsonb) and the GIN index: the text form in CamusJsonb,
 * parameter binding, the REST row decoders, the gRPC codec, the data reader, Dapper, and the EF Core
 * mapping, DDL, GIN index and EF.Functions translations. No server is required.
 */

using System.Data;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using CamusDB.Client.Transport;
using CamusDB.Dapper;
using CamusDB.EntityFrameworkCore;
using Dapper;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Grpc = CamusDB.Grpc;

namespace CamusDB.Client.Tests;

public class TestJsonbValues
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    // ─── CamusJsonb.Format ────────────────────────────────────────────────────

    [Theory]
    [InlineData("""{"a":1,"b":[1,2]}""", """{"a": 1, "b": [1, 2]}""")]
    [InlineData("""  [ ]  """, "[]")]
    [InlineData("{}", "{}")]
    [InlineData("""{"a":{},"b":[[],{}]}""", """{"a": {}, "b": [[], {}]}""")]
    [InlineData("12345678901234567890.50", "12345678901234567890.50")]
    [InlineData("""[1.50, -0.0, 1e3]""", "[1.50, -0.0, 1e3]")]
    [InlineData("\"x\"", "\"x\"")]
    [InlineData("null", "null")]
    [InlineData("""[true,false,null]""", "[true, false, null]")]
    public void FormatGivesThePostgreSqlTextForm(string json, string expected)
    {
        Assert.Equal(expected, CamusJsonb.Format(json));
    }

    [Fact]
    public void FormatEscapesAsPostgreSqlDoes()
    {
        // A quote, a backslash and the control characters are escaped; other characters are kept, also
        // when the input escapes them.
        Assert.Equal("[\"a\\\"b\\\\c\\n\\u0001é😀/\"]", CamusJsonb.Format("[\"a\\\"b\\\\c\\n\\u0001\\u00e9\\ud83d\\ude00\\/\"]"));
        Assert.Equal("{\"k\\tk\": 1}", CamusJsonb.Format("{\"k\\tk\":1}"));
    }

    [Theory]
    [InlineData("")]
    [InlineData("[1,")]
    [InlineData("1 2")]
    public void FormatRefusesTextThatIsNotOneJsonValue(string json)
    {
        Assert.ThrowsAny<JsonException>(() => CamusJsonb.Format(json));
    }

    // ─── Parameter binding ────────────────────────────────────────────────────

    private static ColumnValue Bind(object? value, ColumnType columnType)
    {
        CamusCommand command = new("SELECT 1", new CamusConnectionStringBuilder(ConnString));
        command.Parameters.Add("@p", columnType, value);

        return command.GetCommandParameters(CamusProtocol.Rest)!["@p"];
    }

    private static ColumnValue BindByDbType(object? value, DbType dbType)
    {
        CamusCommand command = new("SELECT 1", new CamusConnectionStringBuilder(ConnString));
        command.Parameters.Add(new CamusParameter { ParameterName = "@p", DbType = dbType, Value = value });

        return command.GetCommandParameters(CamusProtocol.Rest)!["@p"];
    }

    public static TheoryData<object> DocumentValues() => new()
    {
        JsonDocument.Parse("""{"a": [1, 2.50]}"""),
        JsonDocument.Parse("""{"a": [1, 2.50]}""").RootElement,
        JsonNode.Parse("""{"a": [1, 2.50]}""")!,
    };

    [Theory]
    [MemberData(nameof(DocumentValues))]
    public void UntypedDocumentBindsAsJsonbWithCompactText(object document)
    {
        ColumnValue value = Bind(document, ColumnType.Null);

        Assert.Equal(ColumnType.Jsonb, value.Type);
        Assert.Equal("""{"a":[1,2.50]}""", value.StrValue!.Replace(" ", ""));
    }

    [Fact]
    public void JsonNodeKeepsItsCompactText()
    {
        Assert.Equal("""{"a":[1,2.50]}""", Bind(JsonNode.Parse("""{"a": [1, 2.50]}"""), ColumnType.Null).StrValue);
        Assert.Equal("[1,2]", Bind(new JsonArray(1, 2), ColumnType.Null).StrValue);
    }

    [Fact]
    public void StringTypedJsonbBindsItsText()
    {
        ColumnValue value = Bind("""{"kind": "login"}""", ColumnType.Jsonb);

        Assert.Equal(ColumnType.Jsonb, value.Type);
        Assert.Equal("""{"kind": "login"}""", value.StrValue);
    }

    [Fact]
    public void PlainStringStaysAString()
    {
        Assert.Equal(ColumnType.String, Bind("""{"a": 1}""", ColumnType.Null).Type);
    }

    [Fact]
    public void DocumentBoundAsAStringIsCompact()
    {
        // JsonNode.ToString indents an object; the bound text must not.
        ColumnValue value = Bind(JsonNode.Parse("""{"a": 1}"""), ColumnType.String);

        Assert.Equal(ColumnType.String, value.Type);
        Assert.Equal("""{"a":1}""", value.StrValue);
    }

    [Fact]
    public void DbTypeObjectWithAJsonArrayBindsOneDocument()
    {
        // DbType.Object maps to Array, and a JsonArray is enumerable. It is still one document.
        ColumnValue value = BindByDbType(new JsonArray(1, "a"), DbType.Object);

        Assert.Equal(ColumnType.Jsonb, value.Type);
        Assert.Equal("""[1,"a"]""", value.StrValue);
    }

    [Fact]
    public void JsonbParameterReportsDbTypeString()
    {
        Assert.Equal(DbType.String, new CamusParameter("@p", ColumnType.Jsonb, "{}").DbType);
    }

    [Fact]
    public void ArrayOfDocumentsIsRefused()
    {
        CamusException ex = Assert.Throws<CamusException>(
            () => Bind(new List<JsonDocument> { JsonDocument.Parse("1") }, ColumnType.Array));

        Assert.Equal("CADB0400", ex.Code);
        Assert.Contains("ARRAY(JSONB)", ex.Message);
    }

    [Fact]
    public void DefaultJsonElementIsRefused()
    {
        CamusException ex = Assert.Throws<CamusException>(() => Bind(default(JsonElement), ColumnType.Jsonb));
        Assert.Equal("CADB0400", ex.Code);
    }

    [Fact]
    public void NullDocumentBindsAsNull()
    {
        Assert.Equal(ColumnType.Null, Bind(null, ColumnType.Jsonb).Type);
        Assert.Equal(ColumnType.Null, Bind(DBNull.Value, ColumnType.Jsonb).Type);
    }

    [Fact]
    public void JsonbParameterSerializesForRest()
    {
        string json = JsonSerializer.Serialize(Bind("""{"a": 1}""", ColumnType.Jsonb), CamusJsonSerializerContext.Default.ColumnValue);

        Assert.Contains("\"type\":13", json);
        Assert.Contains("\"strValue\":\"{\\u0022a\\u0022: 1}\"", json);
    }

    // ─── REST row decoding ────────────────────────────────────────────────────

    private static CamusDataReader ReaderFor(string rows)
    {
        List<CamusColumnSchema> columns = [new() { Name = "doc", Type = ColumnType.Jsonb }];

        using JsonDocument doc = JsonDocument.Parse(rows);
        return new CamusDataReader(CamusResultSet.FromWire(columns, doc.RootElement.Clone()));
    }

    [Fact]
    public void JsonbCellReadsAsItsPostgreSqlText()
    {
        using CamusDataReader reader = ReaderFor("""[[{"a":[1,12345678901234567890.50],"b":"x"}], [[]], ["s"], [7], [false], [null]]""");

        Assert.Equal(typeof(string), reader.GetFieldType(0));
        Assert.Equal("Jsonb", reader.GetDataTypeName(0));

        Assert.True(reader.Read());
        Assert.Equal(ColumnType.Jsonb, reader.GetColumnValue(0).Type);
        Assert.Equal("""{"a": [1, 12345678901234567890.50], "b": "x"}""", reader.GetString(0));
        Assert.Equal("""{"a": [1, 12345678901234567890.50], "b": "x"}""", reader.GetValue(0));

        Assert.True(reader.Read());
        Assert.Equal("[]", reader.GetString(0));

        Assert.True(reader.Read());
        Assert.Equal("\"s\"", reader.GetString(0));

        Assert.True(reader.Read());
        Assert.Equal("7", reader.GetString(0));

        Assert.True(reader.Read());
        Assert.Equal("false", reader.GetString(0));

        // REST writes SQL NULL and the JSON null document as the same token.
        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(0));
    }

    /// <summary>
    /// A column the server marked <c>jsonbAsText</c> carries each document as a string that holds its
    /// text. JSON null is then the string <c>"null"</c>, and only the token null is SQL NULL. Both
    /// decoders, the buffered one and the streaming one, read the mark from the same schema reader.
    /// </summary>
    [Fact]
    public void MarkedJsonbColumnDecodesTheTextForm_AndKeepsJsonNullApartFromSqlNull()
    {
        const string columns = """[{"name": "doc", "type": 13, "jsonbAsText": true}, {"name": "n", "type": 4}]""";
        string[] rows =
        [
            """["{\"a\": [1, 12345678901234567890.50]}", 1]""",
            """["null", 2]""",
            """["\"abc\"", 3]""",
            """[null, 4]""",
        ];
        string[] expected = ["""{"a": [1, 12345678901234567890.50]}""", "null", "\"abc\""];

        using JsonDocument schema = JsonDocument.Parse(columns);
        using JsonDocument body = JsonDocument.Parse("[" + string.Join(",", rows) + "]");
        using CamusDataReader reader = new(CamusResultSet.FromWire(schema.RootElement.Clone(), body.RootElement.Clone()));

        for (int i = 0; i < expected.Length; i++)
        {
            Assert.True(reader.Read());
            Assert.False(reader.IsDBNull(0));
            Assert.Equal(expected[i], reader.GetString(0));
        }

        Assert.True(reader.Read());
        Assert.True(reader.IsDBNull(0));
        Assert.Equal(4, reader.GetInt64(1));

        (_, ColumnType[] types, ColumnType[]? elementTypes) = CamusResultSet.ReadSchema(schema.RootElement);
        ColumnValue[] cells = new ColumnValue[2];

        for (int i = 0; i < expected.Length; i++)
        {
            Assert.True(CamusResultSet.TryDecodeRowInto(Encoding.UTF8.GetBytes(rows[i]), types, cells, elementTypes));
            Assert.Equal(ColumnType.Jsonb, cells[0].Type);
            Assert.Equal(expected[i], cells[0].StrValue);
        }

        // The typed schema list (the RETURNING path) reads the same mark.
        List<CamusColumnSchema> list = [new() { Name = "doc", Type = ColumnType.Jsonb, JsonbAsText = true }];
        using JsonDocument one = JsonDocument.Parse("""[["null"]]""");
        using CamusDataReader returning = new(CamusResultSet.FromWire(list, one.RootElement.Clone()));
        Assert.True(returning.Read());
        Assert.Equal("null", returning.GetString(0));
    }

    /// <summary>
    /// Without the mark a string cell is a string document, as a server older than <c>jsonbAsText</c>
    /// sends it: the client must not read it as document text.
    /// </summary>
    [Fact]
    public void UnmarkedJsonbColumnKeepsTheNativeForm()
    {
        using CamusDataReader reader = ReaderFor("""[["{\"a\": 1}"]]""");

        Assert.True(reader.Read());
        Assert.Equal("\"{\\\"a\\\": 1}\"", reader.GetString(0));
    }

    [Fact]
    public void JsonbCellParsesIntoTheSystemTextJsonTypes()
    {
        using CamusDataReader reader = ReaderFor("""[[{"price":12345678901234567890.50}], [[1, 2]]]""");

        Assert.True(reader.Read());
        using (JsonDocument document = reader.GetFieldValue<JsonDocument>(0))
            Assert.Equal(12345678901234567890.50m, document.RootElement.GetProperty("price").GetDecimal());

        Assert.Equal("12345678901234567890.50", reader.GetFieldValue<JsonElement>(0).GetProperty("price").GetRawText());
        Assert.Equal(12345678901234567890.50m, reader.GetFieldValue<JsonNode>(0)!["price"]!.GetValue<decimal>());
        Assert.IsType<JsonObject>(reader.GetFieldValue<JsonObject>(0));
        Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<JsonArray>(0));

        Assert.True(reader.Read());
        Assert.Equal(2, reader.GetFieldValue<JsonArray>(0).Count);
    }

    [Fact]
    public void StringCellParsesAsADocumentToo()
    {
        List<CamusColumnSchema> columns = [new() { Name = "t", Type = ColumnType.String }];
        using JsonDocument doc = JsonDocument.Parse("""[["{\"a\": 1}"], [5]]""");
        using CamusDataReader reader = new(CamusResultSet.FromWire(columns, doc.RootElement.Clone()));

        Assert.True(reader.Read());
        Assert.Equal(1, reader.GetFieldValue<JsonElement>(0).GetProperty("a").GetInt32());

        // An int64 cell holds no JSON text.
        Assert.True(reader.Read());
        Assert.Throws<InvalidCastException>(() => reader.GetFieldValue<JsonDocument>(0));
    }

    [Theory]
    [InlineData("""[{"a":[1,{"b":null}]}]""")]
    [InlineData("""[[1.50,"x\ny"]]""")]
    [InlineData("""["é"]""")]
    [InlineData("""[3]""")]
    [InlineData("""[true]""")]
    [InlineData("""[null]""")]
    public void StreamingDecoderAgreesWithTheDomDecoder(string row)
    {
        ColumnType[] types = [ColumnType.Jsonb];

        using JsonDocument doc = JsonDocument.Parse("[" + row + "]");
        CamusResultSet dom = CamusResultSet.FromWire([new() { Name = "doc", Type = ColumnType.Jsonb }], doc.RootElement.Clone());

        ColumnValue[] cells = new ColumnValue[1];
        Assert.True(CamusResultSet.TryDecodeRowInto(Encoding.UTF8.GetBytes(row), types, cells));

        ColumnValue expected = dom.GetCell(0, 0);
        Assert.Equal(expected.Type, cells[0].Type);
        Assert.Equal(expected.StrValue, cells[0].StrValue);
    }

    [Fact]
    public void StreamingDecoderReadsTheCellsAfterADocument()
    {
        ColumnType[] types = [ColumnType.Jsonb, ColumnType.Integer64, ColumnType.Jsonb];
        ColumnValue[] cells = new ColumnValue[3];

        Assert.True(CamusResultSet.TryDecodeRowInto(Encoding.UTF8.GetBytes("""[{"a":{"b":[1]}},42,[{}]]"""), types, cells));

        Assert.Equal("""{"a": {"b": [1]}}""", cells[0].StrValue);
        Assert.Equal(42, cells[1].LongValue);
        Assert.Equal("[{}]", cells[2].StrValue);
    }

    [Fact]
    public void DeeplyNestedDocumentDecodes()
    {
        // The server stores 512 levels; the default reader depth is 64.
        const int depth = 500;
        string document = new string('[', depth) + new string(']', depth);
        string row = "[" + document + "]";

        ColumnValue[] cells = new ColumnValue[1];
        Assert.True(CamusResultSet.TryDecodeRowInto(Encoding.UTF8.GetBytes(row), [ColumnType.Jsonb], cells));
        Assert.Equal(document, cells[0].StrValue);

        using JsonDocument rows = JsonDocument.Parse("[" + row + "]", CamusJsonb.DocumentOptions);
        CamusResultSet dom = CamusResultSet.FromWire([new() { Name = "doc", Type = ColumnType.Jsonb }], rows.RootElement.Clone());
        Assert.Equal(document, dom.GetCell(0, 0).StrValue);

        using CamusDataReader reader = new(dom);
        Assert.True(reader.Read());
        Assert.Equal(JsonValueKind.Array, reader.GetFieldValue<JsonElement>(0).ValueKind);
    }

    // ─── gRPC codec ───────────────────────────────────────────────────────────

    [Fact]
    public void GrpcEncodesJsonbAsText()
    {
        const string text = """{"a": [1, 2.50]}""";
        Grpc.Value encoded = GrpcValueCodec.Encode(new ColumnValue { Type = ColumnType.Jsonb, StrValue = text });

        Assert.Equal(Grpc.Value.KindOneofCase.JsonValue, encoded.KindCase);
        Assert.Equal(text, encoded.JsonValue);

        ColumnValue decoded = GrpcValueCodec.Decode(encoded);
        Assert.Equal(ColumnType.Jsonb, decoded.Type);
        Assert.Equal(text, decoded.StrValue);
    }

    [Fact]
    public void GrpcJsonNullDocumentIsNotSqlNull()
    {
        ColumnValue decoded = GrpcValueCodec.Decode(new Grpc.Value { JsonValue = "null" });

        Assert.Equal(ColumnType.Jsonb, decoded.Type);
        Assert.Equal("null", decoded.StrValue);
    }

    [Fact]
    public void GrpcColumnTypeMatchesTheClientEnum()
    {
        Assert.Equal(Grpc.ColumnType.Jsonb, GrpcValueCodec.ToGrpcColumnType(ColumnType.Jsonb));
        Assert.Equal(ColumnType.Jsonb, GrpcValueCodec.ToClientColumnType(Grpc.ColumnType.Jsonb));
    }

    // ─── Dapper ───────────────────────────────────────────────────────────────

    [Fact]
    public void DapperJsonbValuesBindAsJsonb()
    {
        Assert.Equal(ColumnType.Jsonb, CamusValue.Jsonb("{}").Type);
        Assert.Equal(ColumnType.Jsonb, CamusValue.Jsonb(JsonDocument.Parse("{}")).Type);
        Assert.Equal(ColumnType.Jsonb, CamusValue.Jsonb(JsonNode.Parse("{}")).Type);
        Assert.Equal(ColumnType.Jsonb, CamusValue.Jsonb(JsonDocument.Parse("{}").RootElement).Type);
    }

    [Fact]
    public void DapperJsonbTypeHandlerBindsAndReads()
    {
        CamusJsonbTypeHandler<JsonObject> handler = new();
        CamusParameter parameter = new();

        handler.SetValue(parameter, new JsonObject { ["a"] = 1 });
        Assert.Equal(ColumnType.Jsonb, parameter.ColumnType);

        Assert.Equal(1, handler.Parse("""{"a": 1}""")["a"]!.GetValue<int>());
        Assert.Throws<DataException>(() => handler.Parse("[1]"));

        Assert.Equal(JsonValueKind.Array, new CamusJsonbTypeHandler<JsonElement>().Parse("[1]").ValueKind);
    }

    [Fact]
    public void DapperRegistersTheDocumentHandlers()
    {
        CamusDapper.Register();

        Assert.True(SqlMapper.HasTypeHandler(typeof(JsonDocument)));
        Assert.True(SqlMapper.HasTypeHandler(typeof(JsonElement)));
        Assert.True(SqlMapper.HasTypeHandler(typeof(JsonNode)));
        Assert.True(SqlMapper.HasTypeHandler(typeof(JsonObject)));
        Assert.True(SqlMapper.HasTypeHandler(typeof(JsonArray)));
    }

    // ─── EF Core: mapping and DDL ─────────────────────────────────────────────

    private static DbContextOptions<T> Options<T>() where T : DbContext
        => new DbContextOptionsBuilder<T>().UseCamusDB(ConnString).Options;

    [Fact]
    public void DocumentPropertiesMapToJsonb()
    {
        using JsonContext ctx = new(Options<JsonContext>());
        IEntityType entityType = ctx.GetService<IDesignTimeModel>().Model.FindEntityType(typeof(Event))!;

        foreach (string name in new[] { nameof(Event.Payload), nameof(Event.Element), nameof(Event.Node), nameof(Event.Tags), nameof(Event.Text) })
            Assert.Equal("jsonb", entityType.FindProperty(name)!.GetColumnType());

        Assert.IsType<CamusJsonbTypeMapping>(entityType.FindProperty(nameof(Event.Text))!.GetRelationalTypeMapping());

        string ddl = CamusDatabaseCreator.BuildCreateTableSql(entityType, "ef_events");
        Assert.Contains("`Payload` JSONB NOT NULL", ddl);
        Assert.Contains("`Element` JSONB NOT NULL", ddl);
        Assert.Contains("`Node` JSONB", ddl);
        Assert.Contains("`Tags` JSONB", ddl);
        Assert.Contains("`Text` JSONB", ddl);
    }

    [Fact]
    public void JsonbLiteralIsTheTypedLiteral()
    {
        using JsonContext ctx = new(Options<JsonContext>());
        IRelationalTypeMappingSource source = ctx.GetService<IRelationalTypeMappingSource>();

        Assert.Equal("JSONB '{\"a\":\"it''s\"}'", source.FindMapping(typeof(JsonDocument))!.GenerateSqlLiteral(JsonDocument.Parse("""{"a":"it's"}""")));
        Assert.Equal("JSONB '[1,2]'", source.FindMapping(typeof(string), "json")!.GenerateSqlLiteral("[1,2]"));
        Assert.Equal("jsonb", source.FindMapping("json")!.StoreType);
    }

    [Fact]
    public void JsonbMappingStampsTheParameter()
    {
        using JsonContext ctx = new(Options<JsonContext>());
        RelationalTypeMapping mapping = ctx.GetService<IRelationalTypeMappingSource>().FindMapping(typeof(string), "jsonb")!;

        using CamusCommand command = new("SELECT 1", new CamusConnectionStringBuilder(ConnString));
        CamusParameter parameter = (CamusParameter)mapping.CreateParameter(command, "@p", "{}");

        Assert.Equal(ColumnType.Jsonb, parameter.ColumnType);
    }

    [Fact]
    public void JsonNodeChangeIsTracked()
    {
        using JsonContext ctx = new(Options<JsonContext>());
        Event entity = new() { Id = 1, Payload = JsonDocument.Parse("{}"), Element = JsonDocument.Parse("1").RootElement, Node = new JsonObject { ["a"] = 1 } };

        ctx.Attach(entity);
        entity.Node!["a"] = 2;
        ctx.ChangeTracker.DetectChanges();

        Assert.True(ctx.Entry(entity).Property(e => e.Node).IsModified);
        Assert.False(ctx.Entry(entity).Property(e => e.Payload).IsModified);
    }

    // ─── EF Core: GIN index ───────────────────────────────────────────────────

    private static IReadOnlyList<string> CreateTablesSql(DbContext ctx)
    {
        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        IReadOnlyList<MigrationOperation> operations = ctx.GetService<IMigrationsModelDiffer>()
            .GetDifferences(null, model.GetRelationalModel());

        return ctx.GetService<IMigrationsSqlGenerator>()
            .Generate(operations, model)
            .Select(c => c.CommandText.Trim())
            .ToList();
    }

    [Fact]
    public void GinIndexMigratesAsUsingGin()
    {
        using JsonContext ctx = new(Options<JsonContext>());

        IReadOnlyList<string> sql = CreateTablesSql(ctx);

        Assert.Contains("CREATE INDEX IF NOT EXISTS `IX_ef_events_Payload` ON `ef_events` USING GIN (`Payload`)", sql);
        Assert.DoesNotContain(sql, s => s.Contains("`IX_ef_events_Tags` ON `ef_events` USING GIN"));
    }

    [Fact]
    public void GinIndexFoldedIntoCreateTableIsAnInvertedIndex()
    {
        using FoldedContext ctx = new(Options<FoldedContext>());

        string createChild = Assert.Single(CreateTablesSql(ctx), s => s.StartsWith("CREATE TABLE IF NOT EXISTS `ef_gin_children`"));

        Assert.Contains("INVERTED INDEX `IX_ef_gin_children_Doc` (`Doc`)", createChild);
        Assert.Contains("KEY `IX_ef_gin_children_ParentId` (`ParentId`)", createChild);
    }

    [Fact]
    public void UniqueGinIndexIsRefused()
    {
        using JsonContext ctx = new(Options<JsonContext>());
        CreateIndexOperation operation = new() { Name = "ix", Table = "t", Columns = ["doc"], IsUnique = true };
        operation[CamusAnnotationNames.IndexMethod] = "gin";

        InvalidOperationException ex = Assert.Throws<InvalidOperationException>(
            () => ctx.GetService<IMigrationsSqlGenerator>().Generate([operation], null));
        Assert.Contains("cannot be unique", ex.Message);
    }

    [Fact]
    public void HasMethodNormalizesAndRefusesUnknownMethods()
    {
        ModelBuilder builder = new();
        var index = builder.Entity<Event>().HasIndex(e => e.Payload);

        index.HasMethod("GIN");
        Assert.Equal("gin", index.Metadata.GetMethod());
        Assert.True(index.Metadata.IsGin());

        index.HasMethod("btree");
        Assert.Null(index.Metadata.GetMethod());

        Assert.Throws<ArgumentException>(() => index.HasMethod("hash"));
    }

    [Fact]
    public void GinIndexRendersAsHasMethodInTheSnapshot()
    {
        using JsonContext ctx = new(Options<JsonContext>());
        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        IIndex index = model.FindEntityType(typeof(Event))!.GetIndexes().Single(i => i.IsGin());

#pragma warning disable EF1001 // The dependencies type is marked internal; the design-time host builds it the same way.
        CamusAnnotationCodeGenerator generator = new(
            new AnnotationCodeGeneratorDependencies(ctx.GetService<IRelationalTypeMappingSource>()));
#pragma warning restore EF1001

        var annotations = index.GetAnnotations().ToDictionary(a => a.Name, a => a);
        var fragment = Assert.Single(
            generator.GenerateFluentApiCalls(index, annotations),
            f => f.Method == nameof(CamusIndexBuilderExtensions.HasMethod));

        Assert.Equal("gin", Assert.Single(fragment.Arguments));
    }

    [Fact]
    public void ChangingTheIndexMethodRecreatesTheIndex()
    {
        using JsonContext before = new(Options<JsonContext>());
        using JsonContext after = new(Options<JsonContext>(), gin: false);

        IReadOnlyList<MigrationOperation> operations = after.GetService<IMigrationsModelDiffer>().GetDifferences(
            before.GetService<IDesignTimeModel>().Model.GetRelationalModel(),
            after.GetService<IDesignTimeModel>().Model.GetRelationalModel());

        Assert.Contains(operations, o => o is DropIndexOperation { Name: "IX_ef_events_Payload" });
        Assert.Contains(operations, o => o is CreateIndexOperation { Name: "IX_ef_events_Payload" } c && c[CamusAnnotationNames.IndexMethod] is null);
    }

    // ─── EF Core: EF.Functions ────────────────────────────────────────────────

    [Fact]
    public void JsonContainsTranslatesToTheFunctionForm()
    {
        using JsonContext ctx = new(Options<JsonContext>());

        string sql = ctx.Events.Where(e => EF.Functions.JsonContains(e.Payload, """{"kind": "login"}""")).ToQueryString();

        Assert.Contains("""jsonb_contains(`e`.`Payload`, JSONB '{"kind": "login"}')""", sql);
    }

    [Fact]
    public void JsonContainsWithAParameterBindsJsonb()
    {
        using JsonContext ctx = new(Options<JsonContext>());
        JsonDocument filter = JsonDocument.Parse("""{"kind": "login"}""");

        string sql = ctx.Events.Where(e => EF.Functions.JsonContains(e.Node!, filter)).ToQueryString();

        Assert.Contains("jsonb_contains(`e`.`Node`, @", sql);
    }

    [Fact]
    public void JsonContainedKeepsItsArgumentOrder()
    {
        using JsonContext ctx = new(Options<JsonContext>());

        string sql = ctx.Events.Where(e => EF.Functions.JsonContained("""{"a": 1}""", e.Payload)).ToQueryString();

        Assert.Contains("""jsonb_contained(JSONB '{"a": 1}', `e`.`Payload`)""", sql);
    }

    [Fact]
    public void JsonExistsFunctionsTranslate()
    {
        using JsonContext ctx = new(Options<JsonContext>());
        string[] keys = ["a", "b"];

        Assert.Contains("jsonb_exists(`e`.`Payload`, 'kind')", ctx.Events.Where(e => EF.Functions.JsonExists(e.Payload, "kind")).ToQueryString());
        Assert.Contains("jsonb_exists_any(`e`.`Payload`, @", ctx.Events.Where(e => EF.Functions.JsonExistAny(e.Payload, keys)).ToQueryString());
        Assert.Contains("jsonb_exists_all(`e`.`Text`, @", ctx.Events.Where(e => EF.Functions.JsonExistAll(e.Text!, keys)).ToQueryString());
        Assert.Contains("jsonb_typeof(`e`.`Element`) = 'object'", ctx.Events.Where(e => EF.Functions.JsonTypeof(e.Element) == "object").ToQueryString());
        Assert.Contains("jsonb_contains(`e`.`Element`, JSONB '1')", ctx.Events.Where(e => EF.Functions.JsonContains(e.Element, "1")).ToQueryString());
    }

    [Fact]
    public void JsonFunctionsCannotRunInMemory()
    {
        Assert.Throws<InvalidOperationException>(() => EF.Functions.JsonContains("{}", "{}"));
    }

    public class Event
    {
        public long Id { get; set; }

        public JsonDocument Payload { get; set; } = null!;

        public JsonElement Element { get; set; }

        public JsonNode? Node { get; set; }

        public JsonArray? Tags { get; set; }

        public string? Text { get; set; }
    }

    private class JsonContext(DbContextOptions options, bool gin = true) : DbContext(options)
    {
        public DbSet<Event> Events => Set<Event>();

        public bool Gin { get; } = gin;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Event>(b =>
            {
                b.ToTable("ef_events");
                b.HasKey(e => e.Id);
                b.Property(e => e.Text).HasColumnType("json");

                var payload = b.HasIndex(e => e.Payload);
                if (Gin)
                    payload.HasMethod("gin");

                b.HasIndex(e => e.Tags);
            });
        }

        // Each model differs, so the two contexts of a test cannot share a cached model.
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.ReplaceService<IModelCacheKeyFactory, GinCacheKeyFactory>();
    }

    private sealed class GinCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime)
            => (context.GetType(), context is JsonContext { Gin: true }, designTime);
    }

    public class GinParent
    {
        public long Id { get; set; }
    }

    public class GinChild
    {
        public long Id { get; set; }

        public long ParentId { get; set; }

        public JsonDocument? Doc { get; set; }
    }

    private class FoldedContext(DbContextOptions options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<GinParent>(b =>
            {
                b.ToTable("ef_gin_parents");
                b.HasKey(e => e.Id);
            });

            modelBuilder.Entity<GinChild>(b =>
            {
                b.ToTable("ef_gin_children");
                b.HasKey(e => e.Id);
                b.HasOne<GinParent>().WithMany().HasForeignKey(e => e.ParentId);
                b.HasIndex(e => e.Doc).HasMethod("gin");
            });
        }
    }
}
