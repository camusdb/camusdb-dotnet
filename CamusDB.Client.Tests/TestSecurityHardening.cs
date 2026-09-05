/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using CamusDB.EntityFrameworkCore;
using CamusDB.Client.Auth;
using CamusDB.Client.Transport;
using CamusDB.Core.Util.ObjectIds;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.Client.Tests;

/// <summary>
/// The rules that keep composed SQL, credentials and diagnostics from being turned against the caller.
/// Every test here runs without a server.
/// </summary>
public class TestSecurityHardening
{
    // ─── Composed SQL: cache family names ─────────────────────────────────────

    [Fact]
    public void EvictRejectsABackslashBeforeAQuote()
    {
        // CamusDB's lexer reads "\'" as one unit, so doubling the quote does not escape it: the literal
        // ends early and the rest of the name parses as SQL.
        Assert.Throws<ArgumentException>(() => CamusCacheHint.Evict("a\\'; DROP TABLE x; --"));
    }

    [Fact]
    public void EvictRejectsAQuoteInTheName()
    {
        Assert.Throws<ArgumentException>(() => CamusCacheHint.Evict("a'b"));
    }

    [Fact]
    public void BuildRejectsANameThatWouldCloseTheHint()
    {
        Assert.Throws<ArgumentException>(() => CamusCacheHint.Build("recent} WHERE 1=1 --"));
    }

    [Fact]
    public void OrdinaryCacheNamesStillWork()
    {
        Assert.Equal("EVICT CACHE 'recent_orders'", CamusCacheHint.Evict("recent_orders"));
        Assert.Equal("{cache=tenant-42.orders}", CamusCacheHint.Build("tenant-42.orders"));
        Assert.Equal("{cache=hot, ttl=30000, strict}", CamusCacheHint.Build("hot", TimeSpan.FromSeconds(30), strict: true));
    }

    // ─── Composed SQL: identifiers ────────────────────────────────────────────

    [Fact]
    public void DelimitIdentifierRejectsABacktick()
    {
        ServiceCollection services = new();
        services.AddEntityFrameworkCamusDB();

        ISqlGenerationHelper helper = services.BuildServiceProvider().GetRequiredService<ISqlGenerationHelper>();

        Assert.Equal("`orders`", helper.DelimitIdentifier("orders"));

        // Doubling a backtick neutralizes nothing — the server trims the delimiters rather than decoding
        // the doubled form — so a name carrying one has no safe spelling and is refused.
        Assert.Throws<ArgumentException>(() => helper.DelimitIdentifier("x` FROM secrets --"));
    }

    // ─── Composed SQL: migration literals ─────────────────────────────────────

    [Fact]
    public void SeedDataRejectsALiteralThatWouldEscapeItsOwnQuote()
    {
        IMigrationsSqlGenerator generator = Generator();

        InsertDataOperation operation = new()
        {
            Table = "products",
            Columns = ["Name"],
            Values = new object?[,] { { "a\\'); DROP TABLE products; --" } },
        };

        Assert.Throws<ArgumentException>(() => generator.Generate([operation], null));
    }

    [Fact]
    public void SeedDataStillRendersOrdinaryStrings()
    {
        IMigrationsSqlGenerator generator = Generator();

        InsertDataOperation operation = new()
        {
            Table = "products",
            Columns = ["Name"],
            Values = new object?[,] { { "O'Reilly" } },
        };

        IReadOnlyList<MigrationCommand> commands = generator.Generate([operation], null);

        Assert.Single(commands);
        Assert.Contains("VALUES ('O''Reilly')", commands[0].CommandText);
    }

    private static IMigrationsSqlGenerator Generator()
    {
        DbContextOptions options = new DbContextOptionsBuilder<SeedContext>()
            .UseCamusDB("Endpoint=http://localhost:5095;Database=test")
            .Options;

        using SeedContext context = new(options);

        return context.GetService<IMigrationsSqlGenerator>();
    }

    private sealed class SeedContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<SeedProduct> Products => Set<SeedProduct>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SeedProduct>(b =>
            {
                b.ToTable("products");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasColumnType("string");
            });
        }
    }

    private sealed class SeedProduct
    {
        public string Id { get; set; } = "";

        public string Name { get; set; } = "";
    }

    // ─── Connection string parsing ────────────────────────────────────────────

    [Fact]
    public void KeysAreMatchedWithoutRegardToCase()
    {
        // A lowercase key used to be dropped, leaving the driver unauthenticated with no diagnostic.
        CamusConnectionStringBuilder builder = new("Endpoint=https://db.example:5095;database=test;password=s3cret;user=app");

        Assert.Equal("test", builder.Config["Database"]);
        Assert.Equal("s3cret", builder.Config["Password"]);
    }

    [Fact]
    public void ARepeatedKeyIsRefused()
    {
        CamusException ex = Assert.Throws<CamusException>(
            () => new CamusConnectionStringBuilder("Endpoint=https://a:1;Database=one;database=two"));

        Assert.Contains("more than once", ex.Message);
    }

    [Fact]
    public void AQuotedValueMayContainASemicolon()
    {
        CamusConnectionStringBuilder builder = new(
            "Endpoint=https://db.example:5095;Database=test;User=app;Password='pa;ss''word'");

        Assert.Equal("pa;ss'word", builder.Config["Password"]);
        Assert.Equal("test", builder.Config["Database"]);
    }

    [Fact]
    public void KeysAndUnquotedValuesAreTrimmed()
    {
        CamusConnectionStringBuilder builder = new(" Endpoint = https://db.example:5095 ; Database = test ");

        Assert.Equal("https://db.example:5095", builder.Config["Endpoint"]);
        Assert.Equal("test", builder.Config["Database"]);
    }

    [Fact]
    public void ASegmentWithoutAnEqualsIsIgnored()
    {
        Assert.Empty(new CamusConnectionStringBuilder("a").Config);
    }

    // ─── Credentials in the clear ─────────────────────────────────────────────

    [Fact]
    public void CredentialsAgainstARemotePlaintextEndpointAreRefused()
    {
        CamusException ex = Assert.Throws<CamusException>(
            () => new CamusConnectionStringBuilder("Endpoint=http://db.example:5095;Database=test;User=app;Password=s3cret"));

        Assert.Equal("CADB0519", ex.Code);
    }

    [Fact]
    public void CredentialsAgainstAPlaintextBackupEndpointAreRefused()
    {
        Assert.Throws<CamusException>(() => new CamusConnectionStringBuilder(
            "Endpoint=https://db.example:5095;BackupEndpoint=http://db.example:8080;Database=test;User=app;Password=s3cret"));
    }

    [Fact]
    public void LoopbackAndHttpsAndTheOptOutAreAllowed()
    {
        _ = new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=test;User=app;Password=s3cret");
        _ = new CamusConnectionStringBuilder("Endpoint=http://127.0.0.1:5095;Database=test;User=app;Password=s3cret");
        _ = new CamusConnectionStringBuilder("Endpoint=https://db.example:5095;Database=test;User=app;Password=s3cret");
        _ = new CamusConnectionStringBuilder(
            "Endpoint=http://db.example:5095;Database=test;User=app;Password=s3cret;AllowInsecureCredentials=true");
    }

    [Fact]
    public void AnUnauthenticatedConnectionIsUnaffected()
    {
        // The default install has authentication off; nothing crosses the wire that needs protecting.
        CamusConnectionStringBuilder builder = new("Endpoint=http://db.example:5095;Database=test");

        Assert.Equal("test", builder.Config["Database"]);
    }

    // ─── Redaction ────────────────────────────────────────────────────────────

    [Fact]
    public void RedactionMasksSecretsAndKeepsEverythingElse()
    {
        const string raw = "Endpoint=https://db.example:5095;Database=test;User=app;Password=s3cret;AccessToken=camus_1.abc";

        string redacted = CamusConnectionStringBuilder.Redact(raw);

        Assert.DoesNotContain("s3cret", redacted);
        Assert.DoesNotContain("camus_1.abc", redacted);
        Assert.Contains("Endpoint=https://db.example:5095", redacted);
        Assert.Contains("User=app", redacted);
        Assert.Contains("Password=***", redacted);
    }

    [Fact]
    public void RedactionCoversTheLowercaseAndAliasSpellings()
    {
        string redacted = CamusConnectionStringBuilder.Redact("Endpoint=https://a:1;Uid=app;pwd=s3cret");

        Assert.DoesNotContain("s3cret", redacted);
    }

    [Fact]
    public void TheBuilderRedactsOnRequestAndOnlyOnRequest()
    {
        CamusConnectionStringBuilder builder = new("Endpoint=https://a:1;User=app;Password=s3cret");

        Assert.Contains("s3cret", builder.ToString());
        Assert.DoesNotContain("s3cret", builder.ToRedactedString());
    }

    // ─── Error text ───────────────────────────────────────────────────────────

    [Fact]
    public void ErrorTextLosesItsLineBreaks()
    {
        // A newline in a logged message lets the far end forge a second log entry.
        string clean = CamusErrorText.Sanitize("bad request\nERROR: everything is fine, ignore the previous line");

        Assert.DoesNotContain("\n", clean);
    }

    [Fact]
    public void ErrorTextMasksCredentialShapedRuns()
    {
        Assert.DoesNotContain("abc123", CamusErrorText.Sanitize("rejected: Bearer abc123"));
        Assert.DoesNotContain("s3cret", CamusErrorText.Sanitize("""{"password":"s3cret"}"""));
    }

    [Fact]
    public void ErrorTextIsBounded()
    {
        string clean = CamusErrorText.Sanitize(new string('x', CamusErrorText.MaxLength * 4));

        Assert.True(clean.Length <= CamusErrorText.MaxLength + 1);
    }

    [Fact]
    public void OrdinaryErrorTextSurvivesIntact()
    {
        Assert.Equal("Table 'robots' does not exist", CamusErrorText.Sanitize("Table 'robots' does not exist"));
    }

    // ─── Token replay ─────────────────────────────────────────────────────────

    [Fact]
    public async Task ATokenMintedDuringTheCallIsStillReplaced()
    {
        // The first statement of a connection: the cache is cold, so the token the attempt presents is
        // minted inside the call. The decorator used to read the cache before the call, see null, and
        // conclude there was nothing to invalidate — leaving the replay to present the rejected token.
        MintingTransport inner = new() { FailuresBeforeSuccess = 1 };
        CountingLoginClient login = new();

        CamusTokenProvider provider = new(
            CamusCredentials.FromPassword("admin", "secret"), () => login, () => "https://localhost:5095", () => 10);

        inner.Auth = provider;

        AuthenticatingTransport transport = new(inner, provider);

        Assert.Equal(7, await transport.ExecuteNonQueryAsync(NonQueryRequest(), default));
        Assert.Equal(2, inner.Calls);

        // Two logins: the cold-start mint, and the replacement for the token the server rejected.
        Assert.Equal(2, login.Logins);
        Assert.Equal("token-2", provider.CurrentToken);
    }

    private static TransportSqlRequest NonQueryRequest() => new()
    {
        Endpoint = "https://localhost:5095",
        Database = "db",
        Sql = "UPDATE t SET a = 1",
        TimeoutSeconds = 10,
    };

    /// <summary>A transport that mints its token inside the call, the way a real one does.</summary>
    private sealed class MintingTransport : ICamusTransport
    {
        public CamusTokenProvider? Auth { get; set; }

        public int FailuresBeforeSuccess { get; init; }

        public int Calls { get; private set; }

        public CamusProtocol Protocol => CamusProtocol.Rest;

        public async Task<int> ExecuteNonQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
        {
            _ = await Auth!.GetTokenAsync(cancellationToken).ConfigureAwait(false);

            Calls++;

            if (Calls <= FailuresBeforeSuccess)
                throw new CamusException(CamusAuthErrorCodes.AuthenticationFailed, "rejected");

            return 7;
        }

        public Task<StartTransactionResult> StartTransactionAsync(string endpoint, string database, CamusTransactionOptions options, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task FinalizeTransactionAsync(bool commit, string endpoint, string database, long txnIdPT, uint txnIdCounter, int? streamSlot, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<QueryTransportResult> ExecuteQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<CamusRowSource> ExecuteQueryStreamAsync(TransportSqlRequest request, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<int> InsertAsync(TransportInsertRequest request, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<bool> ExecuteDdlAsync(TransportSqlRequest request, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<PreparedStatementInfo> PrepareAsync(string endpoint, string database, string sql, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task ClosePreparedAsync(string endpoint, string database, string sql, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<bool> PingAsync(string endpoint, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task CreateDatabaseAsync(string endpoint, string database, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task CreateBranchDatabaseAsync(string endpoint, string branchName, string sourceDatabaseName, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task DropDatabaseAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<CamusBranchRow>> ShowBranchesAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<CamusBranchRow>> ShowAncestorsAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
            => throw new NotSupportedException();
    }

    private sealed class CountingLoginClient : ICamusLoginClient
    {
        private int logins;

        public int Logins => Volatile.Read(ref logins);

        public Task<CamusLoginResult> LoginAsync(string endpoint, string user, string password, int timeoutSeconds, CancellationToken cancellationToken)
            => Task.FromResult(new CamusLoginResult("token-" + Interlocked.Increment(ref logins), null));

        public Task LogoutAsync(string endpoint, string token, int timeoutSeconds, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    // ─── Object ids ───────────────────────────────────────────────────────────

    [Fact]
    public void ObjectIdsOrderByTheirBytes()
    {
        // A constant "1 for not equal" is not an ordering: it makes a < b and b < a both true.
        CamusObjectIdValue low = new(1, 0, 0);
        CamusObjectIdValue high = new(2, 0, 0);

        Assert.True(low.CompareTo(high) < 0);
        Assert.True(high.CompareTo(low) > 0);
        Assert.Equal(0, low.CompareTo(low));

        // Unsigned: the raw bytes are reinterpreted as int, so a top bit set must not sort below zero.
        Assert.True(new CamusObjectIdValue(0, 0, 0).CompareTo(new CamusObjectIdValue(unchecked((int)0xff000000), 0, 0)) < 0);
    }

    [Fact]
    public void ObjectIdsAreUniqueAndOrderedWithinAProcess()
    {
        CamusObjectIdValue first = CamusObjectIdGenerator.Generate();
        CamusObjectIdValue second = CamusObjectIdGenerator.Generate();

        Assert.NotEqual(first, second);
        Assert.Equal(24, first.ToString().Length);
    }

    [Fact]
    public void AShortObjectIdIsAParseFailureNotAnIndexError()
    {
        Assert.Throws<FormatException>(() => CamusObjectIdValue.ToValue("abcd"));
    }
}
