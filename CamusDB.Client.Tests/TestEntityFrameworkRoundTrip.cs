/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

// End-to-end EF Core round-trip tests against a running CamusDB server.
//
// Repro for the Vlitz "saving a spec loses the edited changes" bug: a tracked entity is loaded,
// a column is mutated, SaveChangesAsync() is called, and then the row is re-read in a fresh
// DbContext. The re-read must observe the new value. This exercises the provider's UPDATE path
// (CamusUpdateSqlGenerator + the modification-command pipeline) exactly as the Vlitz control-plane
// managers use it (load tracked -> mutate -> SaveChangesAsync).
public class TestEntityFrameworkRoundTrip : BaseTest
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    private static DbContextOptions<SpecContext> Options() =>
        new DbContextOptionsBuilder<SpecContext>().UseCamusDB(ConnString).Options;

    // Repro of the ACTUAL Vlitz mapping style: string columns configured with ONLY `.IsRequired()` /
    // `.HasMaxLength(...)` and NO explicit `.HasColumnType("string")` — relying on the default CLR-string
    // mapping. The runtime UPDATE captured from the portal set only SpecVersion + UpdatedAt and dropped
    // SpecMarkdown, i.e. EF did not detect the string change. If the default string mapping lacks a
    // value-comparer that the explicit "string" mapping has, an edited string is silently not persisted.
    [Fact]
    public async Task TestTrackedUpdateDetectsStringChange_DefaultMapping()
    {
        CamusConnection connection = await GetConnection();
        await connection.CreateDatabaseAsync(ifNotExists: true);

        string table = "defmap_" + Guid.NewGuid().ToString("n");
        await using (CamusCommand cmd = connection.CreateCamusCommand(
            $"CREATE TABLE {table} (id STRING PRIMARY KEY NOT NULL, specMarkdown STRING NOT NULL, specVersion INT64 NOT NULL)"))
            Assert.True(await cmd.ExecuteDDLAsync());

        string id = Guid.NewGuid().ToString("n");

        await using (DefaultMapContext ctx = new(DefaultMapOptions(), table))
        {
            ctx.Docs.Add(new DefaultMapDoc { Id = id, SpecMarkdown = "# Spec\n\noriginal body", SpecVersion = 1 });
            await ctx.SaveChangesAsync();
        }

        await using (DefaultMapContext ctx = new(DefaultMapOptions(), table))
        {
            DefaultMapDoc doc = await ctx.Docs.FirstAsync(d => d.Id == id);
            doc.SpecMarkdown = "# Spec\n\nEDITED body <!-- CAPTURE 123 -->";
            doc.SpecVersion++;
            await ctx.SaveChangesAsync();
        }

        await using (DefaultMapContext ctx = new(DefaultMapOptions(), table))
        {
            DefaultMapDoc reread = await ctx.Docs.AsNoTracking().FirstAsync(d => d.Id == id);
            Assert.Equal(2, reread.SpecVersion);
            Assert.Equal("# Spec\n\nEDITED body <!-- CAPTURE 123 -->", reread.SpecMarkdown);
        }
    }

    private static DbContextOptions<DefaultMapContext> DefaultMapOptions() =>
        new DbContextOptionsBuilder<DefaultMapContext>().UseCamusDB(ConnString).Options;

    private sealed class DefaultMapDoc
    {
        public string Id { get; set; } = "";
        public string SpecMarkdown { get; set; } = "";
        public long SpecVersion { get; set; }
    }

    private sealed class DefaultMapContext(DbContextOptions options, string table) : DbContext(options)
    {
        public DbSet<DefaultMapDoc> Docs => Set<DefaultMapDoc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            // Exactly like Vlitz ControlDbContext DevelopmentFeatures: no HasColumnType anywhere.
            modelBuilder.Entity<DefaultMapDoc>(b =>
            {
                b.ToTable(table);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).IsRequired().HasMaxLength(36).HasColumnName("id");
                b.Property(e => e.SpecMarkdown).IsRequired().HasColumnName("specMarkdown");
                b.Property(e => e.SpecVersion).IsRequired().HasColumnName("specVersion");
            });
        }
    }

    private static async Task<string> CreateSpecTableAsync(CamusConnection connection)
    {
        await connection.CreateDatabaseAsync(ifNotExists: true);

        string table = "specdoc_" + Guid.NewGuid().ToString("n");
        string ddl =
            $"CREATE TABLE {table} (" +
            " id OID PRIMARY KEY NOT NULL," +
            " body STRING NOT NULL," +
            " version INT64," +
            " updatedAt INT64)";

        await using CamusCommand cmd = connection.CreateCamusCommand(ddl);
        Assert.True(await cmd.ExecuteDDLAsync());
        return table;
    }

    // The core repro: load tracked, edit a STRING column (+ bump an int), SaveChanges, re-read fresh.
    [Fact]
    public async Task TestTrackedUpdatePersistsEditedColumns()
    {
        CamusConnection connection = await GetConnection();
        string table = await CreateSpecTableAsync(connection);
        string id = CamusObjectIdGenerator.GenerateAsString();

        // Insert the initial "spec" (mirrors platform-bootstrap seeding of a feature spec).
        await using (SpecContext ctx = new(Options(), table))
        {
            ctx.Docs.Add(new SpecDoc { Id = id, Body = "original spec body", Version = 1, UpdatedAt = 100 });
            await ctx.SaveChangesAsync();
        }

        // Edit in "the portal": load tracked, mutate, save.
        await using (SpecContext ctx = new(Options(), table))
        {
            SpecDoc doc = await ctx.Docs.FirstAsync(d => d.Id == id);
            doc.Body = "EDITED spec body from the portal";
            doc.Version = 2;
            doc.UpdatedAt = 200;
            await ctx.SaveChangesAsync();
        }

        // Reload in a fresh context (mirrors GetFeatureByKey after navigating back).
        await using (SpecContext ctx = new(Options(), table))
        {
            SpecDoc reloaded = await ctx.Docs.AsNoTracking().FirstAsync(d => d.Id == id);

            Assert.Equal(2, reloaded.Version);
            Assert.Equal(200, reloaded.UpdatedAt);
            Assert.Equal("EDITED spec body from the portal", reloaded.Body);
        }
    }

    // Narrower repro: update a single string column only.
    [Fact]
    public async Task TestTrackedUpdatePersistsSingleStringColumn()
    {
        CamusConnection connection = await GetConnection();
        string table = await CreateSpecTableAsync(connection);
        string id = CamusObjectIdGenerator.GenerateAsString();

        await using (SpecContext ctx = new(Options(), table))
        {
            ctx.Docs.Add(new SpecDoc { Id = id, Body = "before", Version = 1, UpdatedAt = 1 });
            await ctx.SaveChangesAsync();
        }

        await using (SpecContext ctx = new(Options(), table))
        {
            SpecDoc doc = await ctx.Docs.FirstAsync(d => d.Id == id);
            doc.Body = "after";
            await ctx.SaveChangesAsync();
        }

        await using (SpecContext ctx = new(Options(), table))
        {
            SpecDoc reloaded = await ctx.Docs.AsNoTracking().FirstAsync(d => d.Id == id);
            Assert.Equal("after", reloaded.Body);
        }
    }

    // Large markdown body (specs are big) — repro if CamusDB truncates/drops large STRING updates.
    [Fact]
    public async Task TestTrackedUpdatePersistsLargeStringColumn()
    {
        CamusConnection connection = await GetConnection();
        string table = await CreateSpecTableAsync(connection);
        string id = CamusObjectIdGenerator.GenerateAsString();

        string bigBefore = "# Spec\n" + string.Concat(Enumerable.Repeat("original paragraph text. ", 800));
        string bigAfter = "# Spec (edited)\n" + string.Concat(Enumerable.Repeat("EDITED paragraph text!! ", 900));

        await using (SpecContext ctx = new(Options(), table))
        {
            ctx.Docs.Add(new SpecDoc { Id = id, Body = bigBefore, Version = 1, UpdatedAt = 1 });
            await ctx.SaveChangesAsync();
        }

        await using (SpecContext ctx = new(Options(), table))
        {
            SpecDoc doc = await ctx.Docs.FirstAsync(d => d.Id == id);
            doc.Body = bigAfter;
            doc.Version = 2;
            await ctx.SaveChangesAsync();
        }

        await using (SpecContext ctx = new(Options(), table))
        {
            SpecDoc reloaded = await ctx.Docs.AsNoTracking().FirstAsync(d => d.Id == id);
            Assert.Equal(2, reloaded.Version);
            Assert.Equal(bigAfter, reloaded.Body);
        }
    }

    // Vlitz control-plane entities use a plain STRING primary key (HasMaxLength(36), not an OID/`id`
    // store type) whose value comes from an app-side id generator. This mirrors DevelopmentFeatures.Id
    // exactly — load tracked by the string key, edit, save, re-read.
    [Fact]
    public async Task TestTrackedUpdatePersistsWithStringPrimaryKey()
    {
        CamusConnection connection = await GetConnection();
        await connection.CreateDatabaseAsync(ifNotExists: true);

        string table = "strkeyspec_" + Guid.NewGuid().ToString("n");
        string ddl =
            $"CREATE TABLE {table} (" +
            " id STRING PRIMARY KEY NOT NULL," +
            " body STRING NOT NULL," +
            " version INT64," +
            " updatedAt INT64)";
        await using (CamusCommand cmd = connection.CreateCamusCommand(ddl))
            Assert.True(await cmd.ExecuteDDLAsync());

        string id = Guid.NewGuid().ToString("n");

        await using (StringKeyContext ctx = new(StringKeyOptions(), table))
        {
            ctx.Docs.Add(new StringKeySpecDoc { Id = id, Body = "original spec body", Version = 1, UpdatedAt = 1 });
            await ctx.SaveChangesAsync();
        }

        await using (StringKeyContext ctx = new(StringKeyOptions(), table))
        {
            StringKeySpecDoc doc = await ctx.Docs.FirstAsync(d => d.Id == id);
            doc.Body = "EDITED spec body from the portal";
            doc.Version = 2;
            doc.UpdatedAt = 2;
            await ctx.SaveChangesAsync();
        }

        await using (StringKeyContext ctx = new(StringKeyOptions(), table))
        {
            StringKeySpecDoc reloaded = await ctx.Docs.AsNoTracking().FirstAsync(d => d.Id == id);
            Assert.Equal(2, reloaded.Version);
            Assert.Equal("EDITED spec body from the portal", reloaded.Body);
        }
    }

    // Vlitz `development_features` has secondary indexes: UNIQUE (gameId, key) and (gameId). This edits a
    // NON-indexed column (body/version) while the indexed columns (gameId, key) stay unchanged — mirroring
    // WriteSpec exactly. Reproduces if the engine fails to persist updates on rows that carry indexes.
    [Fact]
    public async Task TestTrackedUpdatePersistsOnRowWithSecondaryIndexes()
    {
        CamusConnection connection = await GetConnection();
        await connection.CreateDatabaseAsync(ifNotExists: true);

        string table = "idxspec_" + Guid.NewGuid().ToString("n");
        string ddl =
            $"CREATE TABLE {table} (" +
            " id STRING PRIMARY KEY NOT NULL," +
            " gameId STRING NOT NULL," +
            " featureKey STRING NOT NULL," +
            " body STRING NOT NULL," +
            " version INT64)";
        await using (CamusCommand cmd = connection.CreateCamusCommand(ddl))
            Assert.True(await cmd.ExecuteDDLAsync());
        await using (CamusCommand cmd = connection.CreateCamusCommand(
            $"CREATE UNIQUE INDEX uq_{table}_game_key ON {table} (gameId, featureKey)"))
            Assert.True(await cmd.ExecuteDDLAsync());
        await using (CamusCommand cmd = connection.CreateCamusCommand(
            $"CREATE INDEX ix_{table}_game ON {table} (gameId)"))
            Assert.True(await cmd.ExecuteDDLAsync());

        string id = Guid.NewGuid().ToString("n");

        await using (IndexedContext ctx = new(IndexedOptions(), table))
        {
            ctx.Docs.Add(new IndexedSpecDoc
            {
                Id = id, GameId = "game-1", FeatureKey = "parties", Body = "original spec body", Version = 1
            });
            await ctx.SaveChangesAsync();
        }

        // Edit only the NON-indexed columns; gameId + featureKey unchanged.
        await using (IndexedContext ctx = new(IndexedOptions(), table))
        {
            IndexedSpecDoc doc = await ctx.Docs.FirstAsync(d => d.GameId == "game-1" && d.Id == id);
            doc.Body = "EDITED spec body from the portal";
            doc.Version = 2;
            await ctx.SaveChangesAsync();
        }

        await using (IndexedContext ctx = new(IndexedOptions(), table))
        {
            IndexedSpecDoc reloaded = await ctx.Docs.AsNoTracking().FirstAsync(d => d.Id == id);
            Assert.Equal(2, reloaded.Version);
            Assert.Equal("EDITED spec body from the portal", reloaded.Body);
        }
    }

    private static DbContextOptions<IndexedContext> IndexedOptions() =>
        new DbContextOptionsBuilder<IndexedContext>().UseCamusDB(ConnString).Options;

    private sealed class IndexedSpecDoc
    {
        public string Id { get; set; } = "";
        public string GameId { get; set; } = "";
        public string FeatureKey { get; set; } = "";
        public string Body { get; set; } = "";
        public long Version { get; set; }
    }

    private sealed class IndexedContext(DbContextOptions options, string table) : DbContext(options)
    {
        public DbSet<IndexedSpecDoc> Docs => Set<IndexedSpecDoc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<IndexedSpecDoc>(b =>
            {
                b.ToTable(table);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnName("id").HasColumnType("string").HasMaxLength(36).ValueGeneratedNever();
                b.Property(e => e.GameId).HasColumnName("gameId").HasColumnType("string");
                b.Property(e => e.FeatureKey).HasColumnName("featureKey").HasColumnType("string");
                b.Property(e => e.Body).HasColumnName("body").HasColumnType("string");
                b.Property(e => e.Version).HasColumnName("version").HasColumnType("int64");
            });
        }
    }

    private static DbContextOptions<StringKeyContext> StringKeyOptions() =>
        new DbContextOptionsBuilder<StringKeyContext>().UseCamusDB(ConnString).Options;

    private sealed class StringKeySpecDoc
    {
        public string Id { get; set; } = "";
        public string Body { get; set; } = "";
        public long Version { get; set; }
        public long UpdatedAt { get; set; }
    }

    private sealed class StringKeyContext(DbContextOptions options, string table) : DbContext(options)
    {
        public DbSet<StringKeySpecDoc> Docs => Set<StringKeySpecDoc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<StringKeySpecDoc>(b =>
            {
                b.ToTable(table);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnName("id").HasColumnType("string").HasMaxLength(36).ValueGeneratedNever();
                b.Property(e => e.Body).HasColumnName("body").HasColumnType("string");
                b.Property(e => e.Version).HasColumnName("version").HasColumnType("int64");
                b.Property(e => e.UpdatedAt).HasColumnName("updatedAt").HasColumnType("int64");
            });
        }
    }

    // Full replica of Vlitz `development_features`: 14 columns incl. enum-as-int, nullable strings, and
    // both secondary indexes. Edits SpecMarkdown + SpecVersion + UpdatedAt exactly like WriteSpec.
    [Fact]
    public async Task TestTrackedUpdatePersistsOnFullFeatureShape()
    {
        CamusConnection connection = await GetConnection();
        await connection.CreateDatabaseAsync(ifNotExists: true);

        string table = "feat_" + Guid.NewGuid().ToString("n");
        await using (CamusCommand cmd = connection.CreateCamusCommand(
            $"CREATE TABLE {table} (" +
            " id STRING PRIMARY KEY NOT NULL," +
            " gameId STRING NOT NULL," +
            " featureKey STRING NOT NULL," +
            " name STRING NOT NULL," +
            " kind INT64 NOT NULL," +
            " specMarkdown STRING NOT NULL," +
            " specVersion INT64 NOT NULL," +
            " guardrailsMarkdown STRING NOT NULL," +
            " guardrailsVersion INT64 NOT NULL," +
            " leadKind INT64 NOT NULL," +
            " leadAccountId STRING," +
            " leadAgentId STRING," +
            " status INT64 NOT NULL," +
            " archivedAt INT64 NOT NULL," +
            " createdAt INT64 NOT NULL," +
            " updatedAt INT64 NOT NULL)"))
            Assert.True(await cmd.ExecuteDDLAsync());
        await using (CamusCommand cmd = connection.CreateCamusCommand(
            $"CREATE UNIQUE INDEX uq_{table} ON {table} (gameId, featureKey)"))
            Assert.True(await cmd.ExecuteDDLAsync());
        await using (CamusCommand cmd = connection.CreateCamusCommand(
            $"CREATE INDEX ix_{table} ON {table} (gameId)"))
            Assert.True(await cmd.ExecuteDDLAsync());

        string id = Guid.NewGuid().ToString("n");

        await using (FeatureContext ctx = new(FeatureOptions(), table))
        {
            ctx.Features.Add(new Feature
            {
                Id = id, GameId = "game-1", FeatureKey = "parties", Name = "Parties",
                Kind = 0, SpecMarkdown = "# Parties\n\noriginal", SpecVersion = 1,
                GuardrailsMarkdown = "# Guardrails", GuardrailsVersion = 1,
                LeadKind = 0, LeadAccountId = null, LeadAgentId = null,
                Status = 3, ArchivedAt = 0, CreatedAt = 100, UpdatedAt = 100
            });
            await ctx.SaveChangesAsync();
        }

        const string edited = "# Parties\n\nEDITED spec body from the portal — must persist.";
        await using (FeatureContext ctx = new(FeatureOptions(), table))
        {
            Feature f = await ctx.Features.FirstAsync(x => x.GameId == "game-1" && x.Id == id);
            f.SpecMarkdown = edited;
            f.SpecVersion++;
            f.UpdatedAt = 200;
            await ctx.SaveChangesAsync();
        }

        await using (FeatureContext ctx = new(FeatureOptions(), table))
        {
            Feature reread = await ctx.Features.AsNoTracking().FirstAsync(x => x.Id == id);
            Assert.Equal(2, reread.SpecVersion);
            Assert.Equal(200, reread.UpdatedAt);
            Assert.Equal(edited, reread.SpecMarkdown);
        }
    }

    private static DbContextOptions<FeatureContext> FeatureOptions() =>
        new DbContextOptionsBuilder<FeatureContext>().UseCamusDB(ConnString).Options;

    private sealed class Feature
    {
        public string Id { get; set; } = "";
        public string GameId { get; set; } = "";
        public string FeatureKey { get; set; } = "";
        public string Name { get; set; } = "";
        public long Kind { get; set; }
        public string SpecMarkdown { get; set; } = "";
        public long SpecVersion { get; set; }
        public string GuardrailsMarkdown { get; set; } = "";
        public long GuardrailsVersion { get; set; }
        public long LeadKind { get; set; }
        public string? LeadAccountId { get; set; }
        public string? LeadAgentId { get; set; }
        public long Status { get; set; }
        public long ArchivedAt { get; set; }
        public long CreatedAt { get; set; }
        public long UpdatedAt { get; set; }
    }

    private sealed class FeatureContext(DbContextOptions options, string table) : DbContext(options)
    {
        public DbSet<Feature> Features => Set<Feature>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Feature>(b =>
            {
                b.ToTable(table);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnName("id").HasColumnType("string").HasMaxLength(36).ValueGeneratedNever();
                b.Property(e => e.GameId).HasColumnName("gameId").HasColumnType("string");
                b.Property(e => e.FeatureKey).HasColumnName("featureKey").HasColumnType("string");
                b.Property(e => e.Name).HasColumnName("name").HasColumnType("string");
                b.Property(e => e.Kind).HasColumnName("kind").HasColumnType("int64");
                b.Property(e => e.SpecMarkdown).HasColumnName("specMarkdown").HasColumnType("string");
                b.Property(e => e.SpecVersion).HasColumnName("specVersion").HasColumnType("int64");
                b.Property(e => e.GuardrailsMarkdown).HasColumnName("guardrailsMarkdown").HasColumnType("string");
                b.Property(e => e.GuardrailsVersion).HasColumnName("guardrailsVersion").HasColumnType("int64");
                b.Property(e => e.LeadKind).HasColumnName("leadKind").HasColumnType("int64");
                b.Property(e => e.LeadAccountId).HasColumnName("leadAccountId").HasColumnType("string");
                b.Property(e => e.LeadAgentId).HasColumnName("leadAgentId").HasColumnType("string");
                b.Property(e => e.Status).HasColumnName("status").HasColumnType("int64");
                b.Property(e => e.ArchivedAt).HasColumnName("archivedAt").HasColumnType("int64");
                b.Property(e => e.CreatedAt).HasColumnName("createdAt").HasColumnType("int64");
                b.Property(e => e.UpdatedAt).HasColumnName("updatedAt").HasColumnType("int64");
            });
        }
    }

    // Schema created by the PROVIDER'S OWN DDL generator (EnsureCreated), not hand-written DDL — this is
    // how the real Vlitz schema is built (EF migrations). Then the WriteSpec-style tracked update.
    [Fact]
    public async Task TestTrackedUpdatePersistsWhenSchemaCreatedByProvider()
    {
        CamusConnection connection = await GetConnection();
        await connection.CreateDatabaseAsync(ifNotExists: true);

        string table = "provgen_" + Guid.NewGuid().ToString("n");
        string id = Guid.NewGuid().ToString("n");

        await using (ProvGenContext ctx = new(ProvGenOptions(), table))
        {
            await ctx.Database.EnsureCreatedAsync();
            ctx.Docs.Add(new ProvGenDoc { Id = id, Body = "original", Version = 1 });
            await ctx.SaveChangesAsync();
        }

        await using (ProvGenContext ctx = new(ProvGenOptions(), table))
        {
            ProvGenDoc doc = await ctx.Docs.FirstAsync(d => d.Id == id);
            doc.Body = "EDITED via provider-created schema";
            doc.Version = 2;
            await ctx.SaveChangesAsync();
        }

        await using (ProvGenContext ctx = new(ProvGenOptions(), table))
        {
            ProvGenDoc reread = await ctx.Docs.AsNoTracking().FirstAsync(d => d.Id == id);
            Assert.Equal(2, reread.Version);
            Assert.Equal("EDITED via provider-created schema", reread.Body);
        }
    }

    private static DbContextOptions<ProvGenContext> ProvGenOptions() =>
        new DbContextOptionsBuilder<ProvGenContext>().UseCamusDB(ConnString).Options;

    private sealed class ProvGenDoc
    {
        public string Id { get; set; } = "";
        public string Body { get; set; } = "";
        public long Version { get; set; }
    }

    private sealed class ProvGenContext(DbContextOptions options, string table) : DbContext(options)
    {
        public DbSet<ProvGenDoc> Docs => Set<ProvGenDoc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<ProvGenDoc>(b =>
            {
                b.ToTable(table);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnName("id").HasColumnType("string").HasMaxLength(36).ValueGeneratedNever();
                b.Property(e => e.Body).HasColumnName("body").HasColumnType("string");
                b.Property(e => e.Version).HasColumnName("version").HasColumnType("int64");
                b.HasIndex(e => e.Version);
            });
        }
    }

    private sealed class SpecDoc
    {
        public string Id { get; set; } = "";
        public string Body { get; set; } = "";
        public long Version { get; set; }
        public long UpdatedAt { get; set; }
    }

    private sealed class SpecContext(DbContextOptions options, string table) : DbContext(options)
    {
        public DbSet<SpecDoc> Docs => Set<SpecDoc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<SpecDoc>(b =>
            {
                b.ToTable(table);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnName("id").HasColumnType("id").ValueGeneratedNever();
                b.Property(e => e.Body).HasColumnName("body").HasColumnType("string");
                b.Property(e => e.Version).HasColumnName("version").HasColumnType("int64");
                b.Property(e => e.UpdatedAt).HasColumnName("updatedAt").HasColumnType("int64");
            });
        }
    }
}
