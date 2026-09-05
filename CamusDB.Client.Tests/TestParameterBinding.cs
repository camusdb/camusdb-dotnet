/**
 * This file is part of CamusDB
 *
 * Offline coverage for parameter construction (CamusCommand): the typed fast paths for array
 * parameters, the protocol-aware UUID form, and the ordering guarantees of the prepared-statement
 * binder. No server is required — these only build the values a request would carry.
 */

using System.Collections;
using CamusDB.Client.Transport;

namespace CamusDB.Client.Tests;

public class TestParameterBinding
{
    private static CamusCommand Command()
        => new("SELECT 1", new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=test"));

    private static ColumnValue Build(object? value, ColumnType arrayElementType = ColumnType.Null)
    {
        CamusCommand command = Command();
        command.Parameters.Add("@a", ColumnType.Array, value).ArrayElementType = arrayElementType;

        return command.GetCommandParameters(CamusProtocol.Rest)!["@a"];
    }

    private static List<ColumnValue> Elements(ColumnValue array) => array.ArrayValues ?? [];

    // ─── Array parameters ─────────────────────────────────────────────────────

    [Fact]
    public void TypedArraysInferTheirElementTypeAndValues()
    {
        ColumnValue longs = Build(new long[] { 1, -2, 3 });
        Assert.Equal(ColumnType.Integer64, longs.ArrayElementType);
        Assert.Equal(new long[] { 1, -2, 3 }, Elements(longs).Select(e => e.LongValue));

        ColumnValue ints = Build(new[] { 4, 5 });
        Assert.Equal(ColumnType.Integer64, ints.ArrayElementType);
        Assert.Equal(new long[] { 4, 5 }, Elements(ints).Select(e => e.LongValue));

        ColumnValue doubles = Build(new[] { 1.5, -0.25 });
        Assert.Equal(ColumnType.Float64, doubles.ArrayElementType);
        Assert.Equal(new[] { 1.5, -0.25 }, Elements(doubles).Select(e => e.FloatValue));

        ColumnValue bools = Build(new[] { true, false });
        Assert.Equal(ColumnType.Bool, bools.ArrayElementType);
        Assert.Equal(new[] { true, false }, Elements(bools).Select(e => e.BoolValue));

        ColumnValue strings = Build(new[] { "a", "b" });
        Assert.Equal(ColumnType.String, strings.ArrayElementType);
        Assert.Equal(new[] { "a", "b" }, Elements(strings).Select(e => e.StrValue));
    }

    [Fact]
    public void TypedArrayAgreesWithTheGeneralEnumerablePath()
    {
        // The same values as a List<long> take the general path; both must produce the same value.
        ColumnValue viaArray = Build(new long[] { 7, 8, 9 });
        ColumnValue viaEnumerable = Build(new List<long> { 7, 8, 9 });

        Assert.Equal(viaEnumerable.ArrayElementType, viaArray.ArrayElementType);
        Assert.Equal(
            Elements(viaEnumerable).Select(e => e.LongValue),
            Elements(viaArray).Select(e => e.LongValue));
    }

    [Fact]
    public void EmptyTypedArrayInfersNothing()
    {
        ColumnValue empty = Build(Array.Empty<long>());

        Assert.Equal(ColumnType.Null, empty.ArrayElementType);
        Assert.Empty(Elements(empty));
    }

    [Fact]
    public void StringArrayWithNullElementsInfersFromTheFirstNonNullElement()
    {
        ColumnValue values = Build(new string?[] { null, "b", null });

        Assert.Equal(ColumnType.String, values.ArrayElementType);
        Assert.Equal(ColumnType.Null, Elements(values)[0].Type);
        Assert.Equal("b", Elements(values)[1].StrValue);
        Assert.Equal(ColumnType.Null, Elements(values)[2].Type);
    }

    [Fact]
    public void AllNullStringArrayCannotInferItsElementType()
    {
        CamusException ex = Assert.Throws<CamusException>(() => Build(new string?[] { null, null }));

        Assert.Equal("CADB0400", ex.Code);
    }

    [Fact]
    public void ExplicitElementTypeOverridesTheArrayElementType()
    {
        ColumnValue values = Build(new long[] { 1, 2 }, ColumnType.String);

        Assert.Equal(ColumnType.String, values.ArrayElementType);
        Assert.Equal(new[] { "1", "2" }, Elements(values).Select(e => e.StrValue));
    }

    [Fact]
    public void ExplicitElementTypeOnAnEmptyArrayIsKept()
    {
        ColumnValue values = Build(Array.Empty<long>(), ColumnType.Integer64);

        Assert.Equal(ColumnType.Integer64, values.ArrayElementType);
        Assert.Empty(Elements(values));
    }

    [Fact]
    public void MixedObjectArrayKeepsTheGeneralInference()
    {
        ColumnValue values = Build(new object?[] { null, 5L, 6L });

        Assert.Equal(ColumnType.Integer64, values.ArrayElementType);
        Assert.Equal(ColumnType.Null, Elements(values)[0].Type);
        Assert.Equal(5L, Elements(values)[1].LongValue);
    }

    [Fact]
    public void SingleUseEnumerableIsStillEnumeratedOnce()
    {
        ColumnValue values = Build(Counting(3));

        Assert.Equal(ColumnType.Integer64, values.ArrayElementType);
        Assert.Equal(new long[] { 0, 1, 2 }, Elements(values).Select(e => e.LongValue));

        static IEnumerable Counting(int count)
        {
            for (int i = 0; i < count; i++)
                yield return (long)i;
        }
    }

    [Fact]
    public void AnEnumeratorThatThrowsReportsItsOwnFailure()
    {
        Assert.Throws<InvalidOperationException>(() => Build(Failing()));

        static IEnumerable Failing()
        {
            yield return 1L;
            throw new InvalidOperationException("broken enumerator");
        }
    }

    [Fact]
    public void MutationBetweenExecutionsIsVisible()
    {
        CamusCommand command = Command();
        long[] values = [1, 2];
        command.Parameters.Add("@a", ColumnType.Array, values);

        Assert.Equal(1L, Elements(command.GetCommandParameters(CamusProtocol.Rest)!["@a"])[0].LongValue);

        values[0] = 42;

        Assert.Equal(42L, Elements(command.GetCommandParameters(CamusProtocol.Rest)!["@a"])[0].LongValue);
    }

    // ─── UUID parameters ──────────────────────────────────────────────────────

    private static ColumnValue Uuid(Guid value, CamusProtocol protocol)
    {
        CamusCommand command = Command();
        command.Parameters.Add("@id", ColumnType.Uuid, value);

        return command.GetCommandParameters(protocol)!["@id"];
    }

    [Fact]
    public void RestKeepsTheCanonicalUuidString()
    {
        Guid guid = Guid.NewGuid();
        ColumnValue value = Uuid(guid, CamusProtocol.Rest);

        Assert.Equal(guid.ToString(), value.StrValue);
        Assert.Equal(guid, value.AsGuid());
    }

    [Fact]
    public void GrpcCarriesTheHalvesWithoutTheString()
    {
        Guid guid = Guid.NewGuid();
        ColumnValue value = Uuid(guid, CamusProtocol.Grpc);

        Assert.Null(value.StrValue);
        Assert.Equal(guid, value.AsGuid());

        // The codec reads the halves, so the encoded bytes are the same on both protocols.
        Assert.Equal(
            GrpcValueCodec.Encode(Uuid(guid, CamusProtocol.Rest)).UuidValue,
            GrpcValueCodec.Encode(value).UuidValue);
    }

    [Fact]
    public void EmptyGuidEncodesTheSameOnBothProtocols()
    {
        Assert.Equal(
            GrpcValueCodec.Encode(Uuid(Guid.Empty, CamusProtocol.Rest)).UuidValue,
            GrpcValueCodec.Encode(Uuid(Guid.Empty, CamusProtocol.Grpc)).UuidValue);

        Assert.Equal(Guid.Empty, Uuid(Guid.Empty, CamusProtocol.Grpc).AsGuid());
    }

    [Fact]
    public void UuidElementsInsideAnArrayFollowTheSameRule()
    {
        Guid guid = Guid.NewGuid();

        CamusCommand rest = Command();
        rest.Parameters.Add("@a", ColumnType.Array, new[] { guid });
        ColumnValue restValue = rest.GetCommandParameters(CamusProtocol.Rest)!["@a"];

        CamusCommand grpc = Command();
        grpc.Parameters.Add("@a", ColumnType.Array, new[] { guid });
        ColumnValue grpcValue = grpc.GetCommandParameters(CamusProtocol.Grpc)!["@a"];

        Assert.Equal(ColumnType.Uuid, restValue.ArrayElementType);
        Assert.Equal(ColumnType.Uuid, grpcValue.ArrayElementType);
        Assert.Equal(guid.ToString(), Elements(restValue)[0].StrValue);
        Assert.Null(Elements(grpcValue)[0].StrValue);
        Assert.Equal(guid, Elements(grpcValue)[0].AsGuid());
    }

    [Fact]
    public void AUuidGivenAsAStringIsUnaffected()
    {
        Guid guid = Guid.NewGuid();

        CamusCommand command = Command();
        command.Parameters.Add("@id", ColumnType.Uuid, guid.ToString());

        Assert.Equal(guid.ToString(), command.GetCommandParameters(CamusProtocol.Grpc)!["@id"].StrValue);
    }

    // ─── Prepared binding ─────────────────────────────────────────────────────

    [Fact]
    public void BindIntoEmitsInThePublishedOrder()
    {
        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@b"] = new() { Type = ColumnType.Integer64, LongValue = 2 },
            ["@a"] = new() { Type = ColumnType.Integer64, LongValue = 1 },
        };

        List<long> emitted = [];
        PreparedStatementBinder.BindInto(["@a", "@b", "@a"], parameters, emitted, static (list, v) => list.Add(v.LongValue));

        Assert.Equal(new long[] { 1, 2, 1 }, emitted);
    }

    [Fact]
    public void BindIntoIgnoresExtraParameters()
    {
        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@a"] = new() { Type = ColumnType.Integer64, LongValue = 1 },
            ["@unused"] = new() { Type = ColumnType.Integer64, LongValue = 9 },
        };

        List<long> emitted = [];
        PreparedStatementBinder.BindInto(["@a"], parameters, emitted, static (list, v) => list.Add(v.LongValue));

        Assert.Equal(new long[] { 1 }, emitted);
    }

    [Fact]
    public void BindIntoReportsAMissingParameterBeforeEmittingAnything()
    {
        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@a"] = new() { Type = ColumnType.Integer64, LongValue = 1 },
        };

        List<long> emitted = [];

        CamusException ex = Assert.Throws<CamusException>(
            () => PreparedStatementBinder.BindInto(["@a", "@missing"], parameters, emitted, static (list, v) => list.Add(v.LongValue)));

        Assert.Equal("CADB0400", ex.Code);
        Assert.Empty(emitted);
    }

    [Fact]
    public void BindIntoWithNoParametersEmitsNothing()
    {
        List<long> emitted = [];
        PreparedStatementBinder.BindInto([], null, emitted, static (list, v) => list.Add(v.LongValue));

        Assert.Empty(emitted);
    }

    [Fact]
    public void BindIntoMatchesTheListBuildingOverload()
    {
        Dictionary<string, ColumnValue> parameters = new()
        {
            ["@a"] = new() { Type = ColumnType.Integer64, LongValue = 1 },
            ["@b"] = new() { Type = ColumnType.String, StrValue = "two" },
        };

        string[] names = ["@b", "@a"];

        List<ColumnValue> viaList = PreparedStatementBinder.Bind(names, parameters, static v => v);

        List<ColumnValue> viaEmit = [];
        PreparedStatementBinder.BindInto(names, parameters, viaEmit, static (list, v) => list.Add(v));

        Assert.Equal(viaList.Select(v => v.Type), viaEmit.Select(v => v.Type));
        Assert.Equal(viaList.Select(v => v.LongValue), viaEmit.Select(v => v.LongValue));
        Assert.Equal(viaList.Select(v => v.StrValue), viaEmit.Select(v => v.StrValue));
    }
}
