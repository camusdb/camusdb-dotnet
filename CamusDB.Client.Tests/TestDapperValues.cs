/**
 * This file is part of CamusDB
 *
 * Offline coverage for the parameters a CamusValue adds to a command. No server is required — these
 * only build the values a request would carry.
 */

using CamusDB.Client.Transport;
using CamusDB.Dapper;
using Dapper;

namespace CamusDB.Client.Tests;

public class TestDapperValues
{
    private static ColumnValue Bind(CamusValue value)
    {
        CamusCommand command = new("SELECT @v", new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=test"));

        ((SqlMapper.ICustomQueryParameter)value).AddParameter(command, "v");

        return command.GetCommandParameters(CamusProtocol.Rest)!["@v"];
    }

    [Fact]
    public void IdBindsAsAnObjectId()
    {
        string id = CamusDB.Core.Util.ObjectIds.CamusObjectIdGenerator.GenerateAsString();

        ColumnValue value = Bind(CamusValue.Id(id));

        Assert.Equal(ColumnType.Id, value.Type);
        Assert.Equal(id, value.StrValue);
    }

    [Fact]
    public void NullUuidBindsAsNull()
    {
        Assert.Equal(ColumnType.Null, Bind(CamusValue.Uuid(null)).Type);
    }

    [Fact]
    public void EmptyArrayTakesItsElementTypeFromT()
    {
        ColumnValue value = Bind(CamusValue.Array(Array.Empty<Guid>()));

        Assert.Equal(ColumnType.Array, value.Type);
        Assert.Equal(ColumnType.Uuid, value.ArrayElementType);
        Assert.Empty(value.ArrayValues ?? []);
    }

    [Fact]
    public void ArrayWithAnExplicitElementType()
    {
        string id = CamusDB.Core.Util.ObjectIds.CamusObjectIdGenerator.GenerateAsString();

        ColumnValue value = Bind(CamusValue.Array(new[] { id }, ColumnType.Id));

        Assert.Equal(ColumnType.Id, value.ArrayElementType);
        Assert.Equal(id, Assert.Single(value.ArrayValues!).StrValue);
    }

    [Fact]
    public void ArrayOfAnUnsupportedTypeIsRefused()
    {
        Assert.Throws<NotSupportedException>(() => CamusValue.Array(new[] { TimeSpan.Zero }));
    }

    [Fact]
    public void VectorBindsAsPackedBytes()
    {
        float[] vector = [1f, -2.5f];

        ColumnValue value = Bind(CamusValue.Vector(vector));

        Assert.Equal(ColumnType.Bytes, value.Type);
        Assert.Equal(vector, CamusVector.ToFloats(value.BytesValue));
    }

    [Fact]
    public void ElementTypeOnAScalarIsRefused()
    {
        Assert.Throws<ArgumentException>(() => new CamusValue(ColumnType.String, "x", ColumnType.String));
    }
}
