/**
 * This file is part of CamusDB
 *
 * Offline coverage for how a Guid parameter goes on the wire. A Guid is 16 bytes and an object id is
 * 12, so a Guid can never be an object id. DbType.Guid resolves to ColumnType.Id, because the EF "id"
 * mapping sends object-id text that way, and the command used to send a Guid value typed Id as its 36
 * characters of text. The server could never match that value: IN returned no rows, NOT IN returned
 * every row, and = failed. A Guid value now always travels as a Uuid; a string keeps the Id type.
 * No server is required: the EF tests stop each command before it executes.
 */

using System.Data;
using System.Data.Common;
using CamusDB.Client.Transport;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace CamusDB.Client.Tests;

public class TestGuidParameters
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static CamusCommand Command()
        => new("SELECT 1", new CamusConnectionStringBuilder(ConnString));

    // ─── ADO.NET ──────────────────────────────────────────────────────────────

    [Theory]
    [InlineData(CamusProtocol.Rest)]
    [InlineData(CamusProtocol.Grpc)]
    public void GuidWithDbTypeGuid_IsSentAsUuid(CamusProtocol protocol)
    {
        Guid guid = Guid.NewGuid();
        CamusCommand command = Command();
        command.Parameters.Add(new CamusParameter { ParameterName = "@id", DbType = DbType.Guid, Value = guid });

        ColumnValue value = command.GetCommandParameters(protocol)!["@id"];

        Assert.Equal(ColumnType.Uuid, value.Type);
        Assert.Equal(guid, value.AsGuid());
        Assert.Equal(guid, GrpcValueCodec.Decode(GrpcValueCodec.Encode(value)).AsGuid());
    }

    [Fact]
    public void GuidDeclaredAsObjectId_IsSentAsUuid()
    {
        Guid guid = Guid.NewGuid();
        CamusCommand command = Command();
        command.Parameters.Add("@id", ColumnType.Id, guid);

        ColumnValue value = command.GetCommandParameters(CamusProtocol.Rest)!["@id"];

        Assert.Equal(ColumnType.Uuid, value.Type);
        Assert.Equal(guid, value.AsGuid());
    }

    [Fact]
    public void ObjectIdStringWithDbTypeGuid_StaysAnObjectId()
    {
        // The EF "id" string mapping carries DbType.Guid; its values are object-id text.
        CamusCommand command = Command();
        command.Parameters.Add(new CamusParameter { ParameterName = "@id", DbType = DbType.Guid, Value = "68000000000000000000abcd" });

        ColumnValue value = command.GetCommandParameters(CamusProtocol.Rest)!["@id"];

        Assert.Equal(ColumnType.Id, value.Type);
        Assert.Equal("68000000000000000000abcd", value.StrValue);
    }

    [Fact]
    public void GuidAsString_StaysAString()
    {
        Guid guid = Guid.NewGuid();
        CamusCommand command = Command();
        command.Parameters.Add("@id", ColumnType.String, guid);

        ColumnValue value = command.GetCommandParameters(CamusProtocol.Rest)!["@id"];

        Assert.Equal(ColumnType.String, value.Type);
        Assert.Equal(guid.ToString(), value.StrValue);
    }

    [Fact]
    public void GuidArraysDeclaredAsObjectIds_AreUuidArrays()
    {
        Guid[] guids = [Guid.NewGuid(), Guid.NewGuid()];

        foreach (object array in new object[] { guids, guids.ToList(), new object?[] { null, guids[0], guids[1] } })
        {
            CamusCommand command = Command();
            command.Parameters.Add("@ids", ColumnType.Array, array).ArrayElementType = ColumnType.Id;

            ColumnValue value = command.GetCommandParameters(CamusProtocol.Rest)!["@ids"];

            Assert.Equal(ColumnType.Uuid, value.ArrayElementType);
            Assert.All(value.ArrayValues!, e => Assert.True(e.Type is ColumnType.Uuid or ColumnType.Null, e.Type.ToString()));
            Assert.Equal(guids, value.ArrayValues!.Where(e => e.Type == ColumnType.Uuid).Select(e => e.AsGuid()));
        }

        // An object-id array of strings is left alone.
        CamusCommand strings = Command();
        strings.Parameters.Add("@ids", ColumnType.Array, new[] { "68000000000000000000abcd" }).ArrayElementType = ColumnType.Id;
        Assert.Equal(ColumnType.Id, strings.GetCommandParameters(CamusProtocol.Rest)!["@ids"].ArrayElementType);
    }

    // ─── Entity Framework ─────────────────────────────────────────────────────

    /// <summary>
    /// Records the wire parameters of every command EF builds, then stops the command, so a query runs
    /// with no server. The connection open is stopped too.
    /// </summary>
    private sealed class ParameterCapture : DbCommandInterceptor, IDbConnectionInterceptor
    {
        public List<Dictionary<string, ColumnValue>> Commands { get; } = [];

        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result,
            CancellationToken cancellationToken = default)
        {
            Commands.Add(((CamusCommand)command).GetCommandParameters(CamusProtocol.Grpc) ?? []);

            return ValueTask.FromResult(InterceptionResult<DbDataReader>.SuppressWithResult(new DataTable().CreateDataReader()));
        }

        public ValueTask<InterceptionResult> ConnectionOpeningAsync(
            DbConnection connection, ConnectionEventData eventData, InterceptionResult result,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult(InterceptionResult.Suppress());

        public InterceptionResult ConnectionOpening(DbConnection connection, ConnectionEventData eventData, InterceptionResult result)
            => InterceptionResult.Suppress();
    }

    private static async Task<Dictionary<string, ColumnValue>> CaptureAsync(Func<AccountContext, Task> query)
    {
        ParameterCapture capture = new();

        DbContextOptions<AccountContext> options = new DbContextOptionsBuilder<AccountContext>()
            .UseCamusDB(ConnString)
            .AddInterceptors(capture)
            .Options;

        await using (AccountContext context = new(options))
            await query(context);

        Assert.Single(capture.Commands);
        return capture.Commands[0];
    }

    [Theory]
    [InlineData(1)]
    [InlineData(50)]
    [InlineData(100)]
    public async Task ContainsOverAGuidList_OnAUuidColumn_BindsUuidParameters(int count)
    {
        List<Guid> ids = Enumerable.Range(0, count).Select(_ => Guid.NewGuid()).ToList();

        Dictionary<string, ColumnValue> parameters = await CaptureAsync(
            context => context.Accounts.Where(a => ids.Contains(a.Id)).ToListAsync());

        Assert.NotEmpty(parameters);
        Assert.All(parameters.Values, p => Assert.Equal(ColumnType.Uuid, p.Type));
        Assert.Equal(ids.ToHashSet(), parameters.Values.Select(p => p.AsGuid()).ToHashSet());
    }

    [Fact]
    public async Task ContainsOverAGuidList_OnAnObjectIdColumn_BindsUuidParameters()
    {
        // ExternalRef is a Guid declared HasColumnType("id"), so EF maps it with the "id" mapping, whose
        // parameters carry DbType.Guid and no Uuid stamp. The Guid values must still travel as Uuid.
        List<Guid> ids = [Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()];

        Dictionary<string, ColumnValue> parameters = await CaptureAsync(
            context => context.Accounts.Where(a => ids.Contains(a.ExternalRef)).ToListAsync());

        Assert.Equal(3, parameters.Count);
        Assert.All(parameters.Values, p => Assert.Equal(ColumnType.Uuid, p.Type));
    }

    [Fact]
    public async Task EqualityWithAGuid_BindsAUuidParameter()
    {
        Guid id = Guid.NewGuid();

        Dictionary<string, ColumnValue> parameters = await CaptureAsync(
            context => context.Accounts.Where(a => a.Id == id || a.ExternalRef == id).ToListAsync());

        Assert.NotEmpty(parameters);
        Assert.All(parameters.Values, p => Assert.Equal(ColumnType.Uuid, p.Type));
    }

    private sealed class AccountContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Account> Accounts => Set<Account>();

        protected override void OnModelCreating(ModelBuilder mb)
        {
            mb.Entity<Account>(b =>
            {
                b.ToTable("guid_parameter_accounts");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("uuid");
                b.Property(e => e.ExternalRef).HasColumnType("id");
                b.Property(e => e.Name).HasMaxLength(64);
            });
        }
    }

    private sealed class Account
    {
        public Guid Id { get; set; }
        public Guid ExternalRef { get; set; }
        public string Name { get; set; } = "";
    }
}
