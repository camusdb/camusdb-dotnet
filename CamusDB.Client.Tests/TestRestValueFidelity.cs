/**
 * This file is part of CamusDB
 *
 * Offline coverage for the values a REST result carries that a JSON token alone cannot type: array
 * elements (decoded against the column's "elementType"), FLOAT32 cells (read as a float, not a double)
 * and negative zero (in results and in the parameters of a request body). Each REST decode is compared
 * with what the gRPC codec gives for the same value, since the two transports must agree. The HTTP calls
 * are faked with Flurl's HttpTest, so no server is needed.
 */

using System.Text;
using System.Text.Json;
using CamusDB.Client.Transport;
using Flurl.Http.Testing;

namespace CamusDB.Client.Tests;

public class TestRestValueFidelity
{
    // ─── Array elements with an elementType ───────────────────────────────────

    private static readonly Guid SampleGuid = Guid.Parse("0f8fad5b-d9cb-469f-a165-70867728950e");

    /// <summary>
    /// One array per element type: its REST cell as the server writes it, and the elements the gRPC
    /// transport gives for it. A null element is in every case.
    /// </summary>
    private static readonly Dictionary<string, (ColumnType ElementType, string Json, ColumnValue[] Elements)> Arrays = new()
    {
        ["date"] = (ColumnType.Date, "[518177952000000000,null,638000000000000000]",
            [new() { Type = ColumnType.Date, LongValue = 518177952000000000 }, ColumnValue.Null, new() { Type = ColumnType.Date, LongValue = 638000000000000000 }]),

        ["datetime"] = (ColumnType.DateTime, "[638000000123456789,0,null]",
            [new() { Type = ColumnType.DateTime, LongValue = 638000000123456789 }, new() { Type = ColumnType.DateTime, LongValue = 0 }, ColumnValue.Null]),

        ["bytes"] = (ColumnType.Bytes, """["AQID","",null,"+/8="]""",
            [new() { Type = ColumnType.Bytes, BytesValue = [1, 2, 3] }, new() { Type = ColumnType.Bytes, BytesValue = [] }, ColumnValue.Null, new() { Type = ColumnType.Bytes, BytesValue = [0xfb, 0xff] }]),

        ["uuid"] = (ColumnType.Uuid, $"[[{High(SampleGuid)},{Low(SampleGuid)}],null,[0,0]]",
            [Uuid(SampleGuid), ColumnValue.Null, Uuid(Guid.Empty)]),

        ["float64"] = (ColumnType.Float64, "[0,-0,1.5,-2,1E+300,null]",
            [Float64(0.0), Float64(-0.0), Float64(1.5), Float64(-2), Float64(1e300), ColumnValue.Null]),

        ["float32"] = (ColumnType.Float32, "[0,-0,-9.321863E+23,0.1,null]",
            [Float32(0f), Float32(-0f), Float32(-9.321863E+23f), Float32(0.1f), ColumnValue.Null]),

        ["int64"] = (ColumnType.Integer64, "[0,-9223372036854775808,null,9223372036854775807]",
            [Int64(0), Int64(long.MinValue), ColumnValue.Null, Int64(long.MaxValue)]),

        ["bool"] = (ColumnType.Bool, "[true,null,false]",
            [new() { Type = ColumnType.Bool, BoolValue = true }, ColumnValue.Null, new() { Type = ColumnType.Bool, BoolValue = false }]),

        ["string"] = (ColumnType.String, """["a","",null,"é"]""",
            [Str(ColumnType.String, "a"), Str(ColumnType.String, ""), ColumnValue.Null, Str(ColumnType.String, "é")]),

        ["id"] = (ColumnType.Id, """["6849f3aa0000000000000001",null]""",
            [Str(ColumnType.Id, "6849f3aa0000000000000001"), ColumnValue.Null]),

        ["numeric"] = (ColumnType.Numeric, """["1.5","-0.000000001",null]""",
            [Str(ColumnType.Numeric, "1.5"), Str(ColumnType.Numeric, "-0.000000001"), ColumnValue.Null]),

        ["empty"] = (ColumnType.Date, "[]", []),
    };

    public static TheoryData<string> ArrayCases() => [.. Arrays.Keys];

    [Theory]
    [MemberData(nameof(ArrayCases))]
    public async Task ArrayElementsDecodeAsTheGrpcTransportDecodesThem(string name)
    {
        (ColumnType elementType, string json, ColumnValue[] elements) = Arrays[name];

        string columns = $$"""[{"name":"a","type":10,"elementType":{{(int)elementType}}}]""";

        ColumnValue grpc = GrpcValueCodec.Decode(GrpcValueCodec.Encode(
            new ColumnValue { Type = ColumnType.Array, ArrayElementType = elementType, ArrayValues = [.. elements] }));

        AssertSameCell(grpc, DecodeBuffered(columns, json));
        AssertSameCell(grpc, await DecodeStreamedAsync(columns, json));
    }

    [Fact]
    public void ColumnSchemaCarriesTheElementType()
    {
        CamusColumnSchema array = JsonSerializer.Deserialize(
            """{"name":"ad","type":10,"elementType":8}""", CamusJsonSerializerContext.Default.CamusColumnSchema)!;
        CamusColumnSchema scalar = JsonSerializer.Deserialize(
            """{"name":"n","type":2}""", CamusJsonSerializerContext.Default.CamusColumnSchema)!;

        Assert.Equal(ColumnType.Date, array.ElementType);
        Assert.Null(scalar.ElementType);

        using JsonDocument rows = JsonDocument.Parse("[[[518177952000000000],7]]");
        CamusResultSet result = CamusResultSet.FromWire([array, scalar], rows.RootElement);

        ColumnValue cell = result.GetCell(0, 0);
        Assert.Equal(ColumnType.Date, cell.ArrayElementType);
        Assert.Equal(ColumnType.Date, Assert.Single(cell.ArrayValues!).Type);
        Assert.Equal(7, result.GetCell(0, 1).LongValue);
    }

    // ─── Array elements without an elementType (an older server) ─────────────

    public static TheoryData<string, string> UntypedColumns() => new()
    {
        { "absent", """[{"name":"a","type":10}]""" },
        { "array of arrays", """[{"name":"a","type":10,"elementType":10}]""" },
        { "unknown to this client", """[{"name":"a","type":10,"elementType":99}]""" },
        { "not a number", """[{"name":"a","type":10,"elementType":"date"}]""" },
    };

    /// <summary>Without a usable element type the elements are inferred from their JSON tokens, exactly
    /// as before the field existed: a date is an Integer64, bytes a String, a uuid Null, and a whole
    /// number in a float64 array an Integer64.</summary>
    [Theory]
    [MemberData(nameof(UntypedColumns))]
    public async Task WithoutAnElementTypeElementsAreInferredFromTheirTokens(string why, string columns)
    {
        _ = why;

        foreach (Func<string, string, Task<ColumnValue>> decode in Decoders)
        {
            ColumnValue dates = await decode(columns, "[518177952000000000,null]");
            Assert.Equal(ColumnType.Integer64, dates.ArrayElementType);
            Assert.Equal(new[] { ColumnType.Integer64, ColumnType.Null }, dates.ArrayValues!.Select(e => e.Type));
            Assert.Equal(518177952000000000, dates.ArrayValues![0].LongValue);

            ColumnValue bytes = await decode(columns, """["AQID"]""");
            Assert.Equal(ColumnType.String, Assert.Single(bytes.ArrayValues!).Type);
            Assert.Equal("AQID", bytes.ArrayValues![0].StrValue);

            ColumnValue uuids = await decode(columns, "[[1,2]]");
            Assert.Equal(ColumnType.Null, Assert.Single(uuids.ArrayValues!).Type);

            ColumnValue floats = await decode(columns, "[1.5,0,-0]");
            Assert.Equal(new[] { ColumnType.Float64, ColumnType.Integer64, ColumnType.Integer64 }, floats.ArrayValues!.Select(e => e.Type));
            Assert.Equal(ColumnType.Integer64, floats.ArrayElementType);
        }
    }

    // ─── FLOAT32 ──────────────────────────────────────────────────────────────

    /// <summary>
    /// The server writes a float32 as the float's shortest text. Parsed as a double, -9.321863E+23 is
    /// not the stored float; parsed as a float and widened it is, and that is what gRPC gives.
    /// </summary>
    [Theory]
    [InlineData("-9.321863E+23")]
    [InlineData("0.1")]
    [InlineData("3.4028235E+38")]
    [InlineData("1E-45")]
    [InlineData("16777217")]
    [InlineData("-0")]
    public async Task Float32CellsHoldTheStoredFloat(string text)
    {
        float stored = float.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
        ColumnValue grpc = GrpcValueCodec.Decode(GrpcValueCodec.Encode(Float32(stored)));

        const string columns = """[{"name":"f","type":6}]""";

        foreach (Func<string, string, Task<ColumnValue>> decode in Decoders)
        {
            ColumnValue cell = await decode(columns, text);

            Assert.Equal(ColumnType.Float32, cell.Type);
            Assert.Equal(Bits(grpc.FloatValue), Bits(cell.FloatValue));
        }
    }

    [Fact]
    public void Float32CellIsNotTheDoubleOfItsText()
    {
        ColumnValue cell = DecodeBuffered("""[{"name":"f","type":6}]""", "-9.321863E+23");

        Assert.Equal(-9.321863238689577E+23, cell.FloatValue);
        Assert.NotEqual(-9.321863E+23, cell.FloatValue);
    }

    // ─── Negative zero ────────────────────────────────────────────────────────

    [Fact]
    public async Task NegativeZeroFloat64CellsKeepTheirSign()
    {
        foreach (Func<string, string, Task<ColumnValue>> decode in Decoders)
        {
            ColumnValue cell = await decode("""[{"name":"d","type":5}]""", "-0");

            Assert.Equal(ColumnType.Float64, cell.Type);
            Assert.True(double.IsNegative(cell.FloatValue));
        }
    }

    /// <summary>
    /// A -0.0 parameter goes into the request body as <c>-0</c>, so the server's System.Text.Json reads
    /// it back as negative zero — scalar or array element, Float64 or Float32, inline or prepared.
    /// </summary>
    [Fact]
    public async Task NegativeZeroParametersAreSentAsNegativeZero()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9343/execute-sql-query").RespondWith(
            """{"status":"ok","columns":[{"name":"n","type":2}],"rows":[]}""");

        using CamusConnection connection = new(new CamusConnectionStringBuilder("Endpoint=http://localhost:9343;Database=test"));
        await using CamusCommand command = connection.CreateCamusCommand("SELECT n FROM t WHERE d = @d AND f = @f AND @a = a AND @af = af");
        AddNegativeZeroParameters(command);

        await using (await command.ExecuteReaderAsync()) { }

        using JsonDocument body = JsonDocument.Parse(await Assert.Single(httpTest.CallLog).HttpRequestMessage.Content!.ReadAsStringAsync());
        JsonElement parameters = body.RootElement.GetProperty("parameters");

        AssertNegativeZero(parameters.GetProperty("@d"));
        AssertNegativeZero(parameters.GetProperty("@f"));
        AssertNegativeZero(parameters.GetProperty("@a").GetProperty("arrayValues")[0]);
        AssertNegativeZero(parameters.GetProperty("@af").GetProperty("arrayValues")[0]);
    }

    [Fact]
    public async Task NegativeZeroPreparedParametersAreSentAsNegativeZero()
    {
        using HttpTest httpTest = new();
        httpTest.ForCallsTo("http://localhost:9344/prepare-sql-statement").RespondWith(
            """{"status":"ok","statementId":"s1","parameterNames":["@d","@f","@a","@af"]}""");
        httpTest.ForCallsTo("http://localhost:9344/execute-sql-query").RespondWith(
            """{"status":"ok","columns":[{"name":"n","type":2}],"rows":[]}""");

        using CamusConnection connection = new(new CamusConnectionStringBuilder("Endpoint=http://localhost:9344;Database=test"));
        await using CamusCommand command = connection.CreateCamusCommand("SELECT n FROM t WHERE d = @d AND f = @f AND @a = a AND @af = af");
        await command.PrepareAsync();
        AddNegativeZeroParameters(command);

        await using (await command.ExecuteReaderAsync()) { }

        var execute = httpTest.CallLog.Single(c => c.Request.Url.ToString().EndsWith("/execute-sql-query", StringComparison.Ordinal));
        using JsonDocument body = JsonDocument.Parse(await execute.HttpRequestMessage.Content!.ReadAsStringAsync());
        JsonElement positional = body.RootElement.GetProperty("positionalParameters");

        AssertNegativeZero(positional[0]);
        AssertNegativeZero(positional[1]);
        AssertNegativeZero(positional[2].GetProperty("arrayValues")[0]);
        AssertNegativeZero(positional[3].GetProperty("arrayValues")[0]);
    }

    private static void AddNegativeZeroParameters(CamusCommand command)
    {
        command.Parameters.Add("@d", ColumnType.Float64, -0.0);
        command.Parameters.Add("@f", ColumnType.Float32, -0.0f);
        command.Parameters.Add("@a", ColumnType.Array, new[] { -0.0, 1.5 });
        command.Parameters.Add("@af", ColumnType.Array, new[] { -0.0f, 1.5f });
    }

    private static void AssertNegativeZero(JsonElement parameter)
    {
        JsonElement floatValue = parameter.GetProperty("floatValue");

        Assert.Equal("-0", floatValue.GetRawText());
        Assert.True(double.IsNegative(floatValue.GetDouble()));
    }

    // ─── Every REST endpoint ──────────────────────────────────────────────────

    /// <summary>
    /// An ARRAY(DATE) and an ARRAY(FLOAT64) column through each REST route that returns rows: the
    /// buffered query, the streamed query and a write with RETURNING.
    /// </summary>
    [Theory]
    [InlineData("query")]
    [InlineData("stream")]
    [InlineData("returning")]
    public async Task EveryRestEndpointTypesArrayElements(string route)
    {
        const string columns = """[{"name":"ad","type":10,"elementType":8},{"name":"af","type":10,"elementType":5},{"name":"f","type":6}]""";
        const string row = "[[518177952000000000],[0,-0,2.5],-9.321863E+23]";

        using HttpTest httpTest = new();
        string sql = "SELECT ad, af, f FROM t";

        switch (route)
        {
            case "query":
                httpTest.ForCallsTo("http://localhost:9345/execute-sql-query").RespondWith(
                    $$"""{"status":"ok","columns":{{columns}},"rows":[{{row}}]}""");
                break;

            case "stream":
                httpTest.ForCallsTo("http://localhost:9345/execute-sql-query-stream").RespondWith(
                    $$"""{"status":"ok","columns":{{columns}}}""" + "\n" + row + "\n" + """{"status":"ok","total":1}""" + "\n");
                break;

            default:
                sql = "UPDATE t SET n = 1 RETURNING ad, af, f";
                httpTest.ForCallsTo("http://localhost:9345/execute-sql-non-query").RespondWith(
                    $$"""{"status":"ok","rows":1,"columns":{{columns}},"returningRows":[{{row}}]}""");
                break;
        }

        using CamusConnection connection = new(new CamusConnectionStringBuilder("Endpoint=http://localhost:9345;Database=test"));
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        await using CamusDataReader reader = route == "stream"
            ? await command.ExecuteStreamReaderAsync()
            : await command.ExecuteReaderAsync();

        Assert.True(await reader.ReadAsync());

        ColumnValue dates = reader.GetColumnValue(0);
        Assert.Equal(ColumnType.Date, dates.ArrayElementType);
        Assert.Equal(new ColumnValue { Type = ColumnType.Date, LongValue = 518177952000000000 }, Assert.Single(dates.ArrayValues!));
        Assert.Equal(new DateTime(518177952000000000, DateTimeKind.Utc), Assert.Single((object?[])reader.GetValue(0)));

        ColumnValue floats = reader.GetColumnValue(1);
        Assert.Equal(ColumnType.Float64, floats.ArrayElementType);
        Assert.All(floats.ArrayValues!, e => Assert.Equal(ColumnType.Float64, e.Type));
        Assert.True(double.IsNegative(floats.ArrayValues![1].FloatValue));

        Assert.Equal(-9.321863238689577E+23, reader.GetColumnValue(2).FloatValue);
        Assert.Equal(-9.321863E+23f, reader.GetFloat(2));

        Assert.False(await reader.ReadAsync());
    }

    // ─── Helpers ──────────────────────────────────────────────────────────────

    // Both REST row decoders, each given a columns list and the JSON of the row's first cell.
    private static readonly Func<string, string, Task<ColumnValue>>[] Decoders =
    [
        (columns, cell) => Task.FromResult(DecodeBuffered(columns, cell)),
        DecodeStreamedAsync,
    ];

    // The buffered endpoint's decoder (CamusResultSet.FromWire over the response DOM).
    private static ColumnValue DecodeBuffered(string columns, string cell)
    {
        using JsonDocument columnsDoc = JsonDocument.Parse(columns);
        using JsonDocument rowsDoc = JsonDocument.Parse("[[" + cell + "]]");

        return CamusResultSet.FromWire(columnsDoc.RootElement, rowsDoc.RootElement).GetCell(0, 0);
    }

    // The streaming endpoint's decoder: the header goes through the stream source's own schema parse.
    private static async Task<ColumnValue> DecodeStreamedAsync(string columns, string cell)
    {
        string body = $$"""{"status":"ok","columns":{{columns}}}""" + "\n[" + cell + "]\n" + """{"status":"ok","total":1}""" + "\n";

        await using NdjsonStreamRowSource source = await NdjsonStreamRowSource.CreateAsync(
            null, new MemoryStream(Encoding.UTF8.GetBytes(body)), CancellationToken.None);

        Assert.True(source.Read());
        return source.GetCell(0);
    }

    private static long Bits(double value) => BitConverter.DoubleToInt64Bits(value);

    private static long High(Guid guid) => Uuid(guid).UuidHigh;

    private static long Low(Guid guid) => Uuid(guid).LongValue;

    private static ColumnValue Uuid(Guid guid)
    {
        Span<byte> bytes = stackalloc byte[16];
        guid.TryWriteBytes(bytes, bigEndian: true, out _);

        return new ColumnValue
        {
            Type = ColumnType.Uuid,
            UuidHigh = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(bytes[..8]),
            LongValue = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(bytes[8..]),
        };
    }

    private static ColumnValue Float64(double value) => new() { Type = ColumnType.Float64, FloatValue = value };

    private static ColumnValue Float32(float value) => new() { Type = ColumnType.Float32, FloatValue = value };

    private static ColumnValue Int64(long value) => new() { Type = ColumnType.Integer64, LongValue = value };

    private static ColumnValue Str(ColumnType type, string value) => new() { Type = type, StrValue = value };

    private static void AssertSameCell(ColumnValue expected, ColumnValue actual)
    {
        Assert.Equal(expected.Type, actual.Type);
        Assert.Equal(expected.LongValue, actual.LongValue);
        Assert.Equal(expected.UuidHigh, actual.UuidHigh);
        Assert.Equal(Bits(expected.FloatValue), Bits(actual.FloatValue));
        Assert.Equal(expected.BoolValue, actual.BoolValue);
        Assert.Equal(expected.StrValue, actual.StrValue);
        Assert.Equal(expected.UuidValue, actual.UuidValue);
        Assert.Equal(expected.BytesValue, actual.BytesValue);
        Assert.Equal(expected.ArrayElementType, actual.ArrayElementType);
        Assert.Equal(expected.ArrayValues is null, actual.ArrayValues is null);

        if (expected.ArrayValues is not null)
        {
            Assert.Equal(expected.ArrayValues.Count, actual.ArrayValues!.Count);

            for (int i = 0; i < expected.ArrayValues.Count; i++)
                AssertSameCell(expected.ArrayValues[i], actual.ArrayValues[i]);
        }
    }
}
