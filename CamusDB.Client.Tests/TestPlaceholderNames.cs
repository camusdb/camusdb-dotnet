/**
 * This file is part of CamusDB
 *
 * Offline coverage for how a bound parameter name reaches the server. The server matches a placeholder
 * by its full token (@name), so a SQL command sends a bare name with the sigil added, and an insert
 * command, whose names are columns, sends each name as it is. No server is required.
 */

using CamusDB.Client.Transport;

namespace CamusDB.Client.Tests;

public class TestPlaceholderNames
{
    private static readonly CamusConnectionStringBuilder Builder = new("Endpoint=http://localhost:5095;Database=test");

    [Fact]
    public void SqlCommandAddsTheSigilToABareName()
    {
        CamusCommand command = new("SELECT * FROM robots WHERE year = @year", Builder);
        command.Parameters.Add("year", ColumnType.Integer64, 1977);

        Dictionary<string, ColumnValue> parameters = command.GetCommandParameters(CamusProtocol.Rest)!;

        Assert.Equal(["@year"], parameters.Keys);
        Assert.Equal(1977, parameters["@year"].LongValue);
    }

    [Fact]
    public void SqlCommandKeepsANameThatHasTheSigil()
    {
        CamusCommand command = new("SELECT * FROM robots WHERE year = @year", Builder);
        command.Parameters.Add("@year", ColumnType.Integer64, 1977);

        Assert.Equal(["@year"], command.GetCommandParameters(CamusProtocol.Rest)!.Keys);
    }

    [Fact]
    public void BothSpellingsOfOneNameAreRefused()
    {
        CamusCommand command = new("SELECT @year", Builder);
        command.Parameters.Add("year", ColumnType.Integer64, 1);
        command.Parameters.Add("@year", ColumnType.Integer64, 2);

        CamusException ex = Assert.Throws<CamusException>(() => command.GetCommandParameters(CamusProtocol.Rest));

        Assert.Contains("'@year'", ex.Message);
    }

    [Fact]
    public void InsertCommandSendsColumnNamesUnchanged()
    {
        CamusInsertCommand command = new("robots", Builder);
        command.Parameters.Add("name", ColumnType.String, "R2-D2");

        Assert.Equal(["name"], command.GetCommandParameters(CamusProtocol.Rest)!.Keys);
    }

    [Fact]
    public void RenamedParameterSendsItsNewName()
    {
        // The placeholder form is kept between executions, so a rename must replace it.
        CamusCommand command = new("SELECT @b", Builder);
        CamusParameter parameter = command.Parameters.Add("a", ColumnType.Integer64, 1);

        Assert.Equal(["@a"], command.GetCommandParameters(CamusProtocol.Rest)!.Keys);

        parameter.ParameterName = "b";
        Assert.Equal(["@b"], command.GetCommandParameters(CamusProtocol.Rest)!.Keys);

        parameter.ParameterName = "@c";
        Assert.Equal(["@c"], command.GetCommandParameters(CamusProtocol.Rest)!.Keys);
    }
}
