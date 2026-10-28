/**
 * This file is part of CamusDB
 *
 * Live coverage for CreateBranchDatabaseAsync, ShowBranchesAsync and ShowAncestorsAsync on both
 * transports. Opt-in: set CAMUSDB_TEST_BRANCHING=true. The REST endpoint comes from
 * CAMUSDB_TEST_ENDPOINT (default localhost:5095), and the gRPC endpoint from CAMUSDB_TEST_GRPC_ENDPOINT
 * (default localhost:5096).
 *
 * Against a server with authentication on, also set CAMUSDB_TEST_USER / CAMUSDB_TEST_PASSWORD to the
 * bootstrap superuser. The tests then connect as that user, and the privilege test runs too.
 */

namespace CamusDB.Client.Tests;

public class TestDatabaseBranchingLive
{
    private static bool Enabled
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_BRANCHING"), "true", StringComparison.OrdinalIgnoreCase);

    private static readonly string? User = Environment.GetEnvironmentVariable("CAMUSDB_TEST_USER");
    private static readonly string? Password = Environment.GetEnvironmentVariable("CAMUSDB_TEST_PASSWORD");

    private static bool Authenticated => !string.IsNullOrEmpty(User) && !string.IsNullOrEmpty(Password);

    private static string ConnString(string protocol, string database, string? user = null, string? password = null)
    {
        string endpoint = protocol == "grpc"
            ? $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096"};Protocol=grpc"
            : $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095"}";

        user ??= User;
        password ??= Password;

        string credentials = !string.IsNullOrEmpty(user) && !string.IsNullOrEmpty(password)
            ? $";User={user};Password={password}"
            : "";

        return $"{endpoint};Database={database}{credentials}";
    }

    private static async Task<CamusConnection> OpenAsync(string protocol, string database, string? user = null, string? password = null)
    {
        CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString(protocol, database, user, password)));
        await connection.OpenAsync();
        return connection;
    }

    private static string NewName(string prefix) => prefix + "_" + Guid.NewGuid().ToString("n")[..12];

    /// <summary>Creates a source database with one table and one row, and returns its name.</summary>
    private static async Task<string> CreateSourceAsync(string protocol)
    {
        string source = NewName("brs");

        await using CamusConnection connection = await OpenAsync(protocol, source);
        await connection.CreateDatabaseAsync(source);

        await using (CamusCommand create = connection.CreateCamusCommand("CREATE TABLE items (id INT64 PRIMARY KEY NOT NULL, name STRING NOT NULL)"))
            await create.ExecuteDDLAsync();

        await using (CamusCommand insert = connection.CreateCamusCommand("INSERT INTO items (id, name) VALUES (1, 'before fork')"))
            Assert.Equal(1, await insert.ExecuteNonQueryAsync());

        return source;
    }

    /// <summary>Drops the databases in order. A parent with a live branch cannot be dropped (CADB0508),
    /// so the caller lists the branches first.</summary>
    private static async Task DropAsync(string protocol, params string[] databases)
    {
        await using CamusConnection connection = await OpenAsync(protocol, databases[^1]);

        foreach (string database in databases)
        {
            try
            {
                await connection.DropDatabaseAsync(database);
            }
            catch (CamusException ex) when (ex.Code == "CADB0010")
            {
                // The test did not get as far as creating it.
            }
        }
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task CreatesABranchThatSeesTheSourceRows(string protocol)
    {
        if (!Enabled)
            return;

        string source = await CreateSourceAsync(protocol);
        string branch = NewName("brb");

        try
        {
            await using (CamusConnection connection = await OpenAsync(protocol, source))
                await connection.CreateBranchDatabaseAsync(branch, source);

            await using CamusConnection onBranch = await OpenAsync(protocol, branch);
            await using CamusCommand select = onBranch.CreateCamusCommand("SELECT name FROM items WHERE id = 1");

            Assert.Equal("before fork", await select.ExecuteScalarAsync());
        }
        finally
        {
            await DropAsync(protocol, branch, source);
        }
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ASecondCreateFailsUnlessIfNotExists(string protocol)
    {
        if (!Enabled)
            return;

        string source = await CreateSourceAsync(protocol);
        string branch = NewName("brb");

        try
        {
            await using CamusConnection connection = await OpenAsync(protocol, source);
            await connection.CreateBranchDatabaseAsync(branch, source);

            CamusException ex = await Assert.ThrowsAsync<CamusException>(() => connection.CreateBranchDatabaseAsync(branch, source));
            Assert.Equal("CADB0012", ex.Code);

            await connection.CreateBranchDatabaseAsync(branch, source, ifNotExists: true);
        }
        finally
        {
            await DropAsync(protocol, branch, source);
        }
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task AnUnknownSourceFailsWithTheServerCode(string protocol)
    {
        if (!Enabled)
            return;

        string branch = NewName("brb");

        // The connection names a database that does not exist: the branch operations do not need one.
        await using CamusConnection connection = await OpenAsync(protocol, branch);

        CamusException ex = await Assert.ThrowsAsync<CamusException>(() => connection.CreateBranchDatabaseAsync(branch, NewName("missing")));
        Assert.Equal("CADB0010", ex.Code);

        ex = await Assert.ThrowsAsync<CamusException>(() => connection.ShowBranchesAsync(NewName("missing")));
        Assert.Equal("CADB0010", ex.Code);
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task ShowsTheBranchTreeInBothDirections(string protocol)
    {
        if (!Enabled)
            return;

        string source = await CreateSourceAsync(protocol);
        string branch = NewName("brb");

        try
        {
            await using CamusConnection connection = await OpenAsync(protocol, source);
            await connection.CreateBranchDatabaseAsync(branch, source);

            CamusBranchRow child = Assert.Single(await connection.ShowBranchesAsync(source));
            Assert.Equal(branch, child.Database);
            Assert.Equal(1, child.Depth);
            Assert.Equal(source, child.Parent);
            Assert.False(string.IsNullOrEmpty(child.Id));
            Assert.False(string.IsNullOrEmpty(child.ForkTimestamp));

            CamusBranchRow parent = Assert.Single(await connection.ShowAncestorsAsync(branch));
            Assert.Equal(source, parent.Database);
            Assert.Equal(1, parent.Depth);
            Assert.Null(parent.Parent);
            Assert.False(string.IsNullOrEmpty(parent.Id));
            Assert.Equal(child.ForkTimestamp, parent.ForkTimestamp);

            // A leaf has no branches, and a root has no ancestors.
            Assert.Empty(await connection.ShowBranchesAsync(branch));
            Assert.Empty(await connection.ShowAncestorsAsync(source));
        }
        finally
        {
            await DropAsync(protocol, branch, source);
        }
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task OnlyASuperuserCreatesABranch(string protocol)
    {
        if (!Enabled || !Authenticated)
            return;

        string source = await CreateSourceAsync(protocol);
        string branch = NewName("brb");
        string user = NewName("bru");
        const string password = "Branch-pass-12345";

        await using CamusConnection admin = await OpenAsync(protocol, source);

        try
        {
            await using (CamusCommand create = admin.CreateCamusCommand($"CREATE USER {user} IDENTIFIED BY '{password}'"))
                await create.ExecuteDDLAsync();

            await using (CamusCommand grant = admin.CreateCamusCommand($"GRANT SELECT ON {source}.* TO {user}"))
                await grant.ExecuteDDLAsync();

            await admin.CreateBranchDatabaseAsync(branch, source);

            await using CamusConnection reader = await OpenAsync(protocol, source, user, password);

            CamusException ex = await Assert.ThrowsAsync<CamusException>(() => reader.CreateBranchDatabaseAsync(NewName("brb"), source));
            Assert.Equal("CADB0517", ex.Code);

            // The SHOW statements need no privilege, but they list only the databases the user can see:
            // this user has no grant on the branch.
            Assert.Empty(await reader.ShowBranchesAsync(source));
            Assert.Empty(await reader.ShowAncestorsAsync(source));

            ex = await Assert.ThrowsAsync<CamusException>(() => reader.ShowAncestorsAsync(branch));
            Assert.Equal("CADB0010", ex.Code);

            // The superuser sees the whole tree.
            Assert.Single(await admin.ShowBranchesAsync(source));
        }
        finally
        {
            try
            {
                await using CamusCommand drop = admin.CreateCamusCommand($"DROP USER {user}");
                await drop.ExecuteDDLAsync();
            }
            catch (CamusException)
            {
                // The test did not get as far as creating it.
            }

            await DropAsync(protocol, branch, source);
        }
    }
}
