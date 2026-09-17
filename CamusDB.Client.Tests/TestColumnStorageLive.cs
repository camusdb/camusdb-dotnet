/**
 * This file is part of CamusDB
 *
 * End-to-end coverage for large-value storage (compression and out-of-line values) against a real
 * server. Opt-in: the whole class no-ops unless CAMUSDB_TEST_LARGE_VALUES=true, because a server that
 * predates large-value storage refuses the STORAGE clause as a parse error.
 *
 * To run it, start a server with large-value storage, then:
 *   export CAMUSDB_TEST_LARGE_VALUES=true
 *   export CAMUSDB_TEST_ENDPOINT=http://localhost:5095        # optional, this is the default
 *   export CAMUSDB_TEST_GRPC_ENDPOINT=http://localhost:5096   # optional, this is the default
 *   dotnet test --filter FullyQualifiedName~TestColumnStorageLive
 */

using CamusDB.Core.Util.ObjectIds;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CamusDB.Client.Tests;

public sealed class TestColumnStorageLive
{
    private static bool Configured
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_LARGE_VALUES"), "true", StringComparison.OrdinalIgnoreCase);

    private static string RestEndpoint
        => Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095";

    private static string GrpcEndpoint
        => Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096";

    private static string ConnectionString(string protocol)
        => protocol == "grpc"
            ? $"Endpoint={GrpcEndpoint};Database=test;Protocol=grpc"
            : $"Endpoint={RestEndpoint};Database=test";

    private static async Task<CamusConnection> ConnectAsync(string protocol)
    {
        CamusConnection connection = new(new CamusConnectionStringBuilder(ConnectionString(protocol)));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync(ifNotExists: true);
        return connection;
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task TestLargeValuesRoundTripThroughEveryStorageForm(string protocol)
    {
        if (!Configured)
            return;

        await using CamusConnection connection = await ConnectAsync(protocol);
        string table = "lv_" + Guid.NewGuid().ToString("n");

        await ExecuteDdlAsync(connection,
            $"CREATE TABLE {table} (" +
            " id OID PRIMARY KEY NOT NULL," +
            " title STRING," +
            " body STRING STORAGE EXTENDED," +
            " image BYTES STORAGE EXTERNAL," +
            " embedding BYTES(3072) STORAGE PLAIN," +
            " tags ARRAY(STRING) STORAGE MAIN)");

        // A compressible body far above the out-of-line threshold, an incompressible image, an embedding
        // above the threshold that PLAIN keeps inline, and a compressible array.
        string id = CamusObjectIdGenerator.GenerateAsString();
        string body = string.Concat(Enumerable.Repeat("CamusDB stores large values compressed or out of line. ", 4000));
        byte[] image = RandomBytes(100_000, seed: 1);
        byte[] embedding = RandomBytes(3072, seed: 2);
        string[] tags = Enumerable.Range(0, 400).Select(i => $"tag-{i % 7}-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").ToArray();

        await using (CamusCommand insert = connection.CreateInsertCommand(table))
        {
            insert.Parameters.Add("id", ColumnType.Id, id);
            insert.Parameters.Add("title", ColumnType.String, "first");
            insert.Parameters.Add("body", ColumnType.String, body);
            insert.Parameters.Add("image", ColumnType.Bytes, image);
            insert.Parameters.Add("embedding", ColumnType.Bytes, embedding);
            insert.Parameters.Add("tags", ColumnType.String, tags, isArray: true);

            Assert.Equal(1, await insert.ExecuteNonQueryAsync());
        }

        await AssertRowAsync(connection, table, id, "first", body, image, embedding, tags);

        // An update of a small column carries the out-of-line pointers; the large values must survive.
        await using (CamusCommand update = connection.CreateCamusCommand($"UPDATE {table} SET title = @title WHERE id = @id"))
        {
            update.Parameters.Add("@title", ColumnType.String, "second");
            update.Parameters.Add("@id", ColumnType.Id, id);
            Assert.Equal(1, await update.ExecuteNonQueryAsync());
        }

        await AssertRowAsync(connection, table, id, "second", body, image, embedding, tags);

        string createTable = await ShowCreateTableAsync(connection, table);
        Assert.Contains("STORAGE EXTENDED", createTable);
        Assert.Contains("STORAGE EXTERNAL", createTable);
        Assert.Contains("STORAGE PLAIN", createTable);
        Assert.Contains("STORAGE MAIN", createTable);

        // SET STORAGE changes future writes only; REWRITE STORAGE converts the stored rows. Neither may
        // change a value.
        await ExecuteDdlAsync(connection, $"ALTER TABLE {table} ALTER COLUMN image SET STORAGE PLAIN");
        Assert.Contains("`image` BYTES NULL STORAGE PLAIN", await ShowCreateTableAsync(connection, table));

        await ExecuteDdlAsync(connection, $"ALTER TABLE {table} REWRITE STORAGE");
        await AssertRowAsync(connection, table, id, "second", body, image, embedding, tags);

        await ExecuteDdlAsync(connection, $"ALTER TABLE {table} REWRITE STORAGE INLINE");
        await AssertRowAsync(connection, table, id, "second", body, image, embedding, tags);
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task TestStorageOnFixedWidthColumnIsRefused(string protocol)
    {
        if (!Configured)
            return;

        await using CamusConnection connection = await ConnectAsync(protocol);
        string table = "lv_bad_" + Guid.NewGuid().ToString("n");

        CamusException ex = await Assert.ThrowsAsync<CamusException>(() => ExecuteDdlAsync(connection,
            $"CREATE TABLE {table} (id OID PRIMARY KEY NOT NULL, year INT64 STORAGE PLAIN)"));

        Assert.Equal("CADB0414", ex.Code);
    }

    [Fact]
    public async Task TestEntityFrameworkEnsureCreatedAndRewrite()
    {
        if (!Configured)
            return;

        await using (CamusConnection connection = await ConnectAsync("rest")) { }

        string table = "lv_ef_" + Guid.NewGuid().ToString("n");
        string id = CamusObjectIdGenerator.GenerateAsString();
        string body = string.Concat(Enumerable.Repeat("An article body that compresses well. ", 3000));
        byte[] image = RandomBytes(50_000, seed: 3);
        byte[] embedding = RandomBytes(3072, seed: 4);

        await using (DocsContext ctx = new(DocsOptions(), table))
        {
            await ctx.Database.EnsureCreatedAsync();

            ctx.Docs.Add(new Doc { Id = id, Title = "t", Body = body, Image = image, Embedding = embedding });
            await ctx.SaveChangesAsync();
        }

        await using (CamusConnection connection = await ConnectAsync("rest"))
        {
            string createTable = await ShowCreateTableAsync(connection, table);
            Assert.Contains("STORAGE EXTERNAL", createTable);
            Assert.Contains("STORAGE PLAIN", createTable);
        }

        await using (DocsContext ctx = new(DocsOptions(), table))
        {
            await ctx.Database.RewriteStorageAsync(table);

            Doc doc = await ctx.Docs.AsNoTracking().SingleAsync(d => d.Id == id);
            Assert.Equal(body, doc.Body);
            Assert.Equal(image, doc.Image);
            Assert.Equal(embedding, doc.Embedding);

            await ctx.Database.RewriteStorageAsync(table, inline: true);

            doc = await ctx.Docs.AsNoTracking().SingleAsync(d => d.Id == id);
            Assert.Equal(body, doc.Body);
            Assert.Equal(image, doc.Image);
        }
    }

    [Fact]
    public async Task TestMigrationSqlRunsOnServer()
    {
        if (!Configured)
            return;

        string table = "lv_mig_" + Guid.NewGuid().ToString("n");

        await using CamusConnection connection = await ConnectAsync("rest");
        await using DocsContext ctx = new(DocsOptions(), table);

        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        IMigrationsSqlGenerator generator = ctx.GetService<IMigrationsSqlGenerator>();

        List<MigrationOperation> operations = [.. ctx.GetService<IMigrationsModelDiffer>().GetDifferences(null, model.GetRelationalModel())];

        // The strategy change a later HasStorage edit produces, then the rewrite a migration adds by hand.
        AlterColumnOperation alter = new()
        {
            Table = table,
            Name = "Embedding",
            ClrType = typeof(byte[]),
            ColumnType = "bytes(3072)",
            IsNullable = true,
            OldColumn = new AddColumnOperation { Table = table, Name = "Embedding", ClrType = typeof(byte[]), ColumnType = "bytes(3072)", IsNullable = true },
        };
        alter.AddAnnotation(CamusAnnotationNames.ColumnStorage, CamusColumnStorage.External);
        alter.OldColumn.AddAnnotation(CamusAnnotationNames.ColumnStorage, CamusColumnStorage.Plain);
        operations.Add(alter);

        MigrationBuilder builder = new("CamusDB");
        builder.RewriteStorage(table);
        operations.AddRange(builder.Operations);

        foreach (MigrationCommand command in generator.Generate(operations, model))
            await ExecuteDdlAsync(connection, command.CommandText.Trim());

        // SHOW CREATE TABLE does not render the BYTES(N) size today, so accept both spellings.
        Assert.Matches(@"`Embedding` BYTES(\(3072\))? NULL STORAGE EXTERNAL", await ShowCreateTableAsync(connection, table));
    }

    private static DbContextOptions<DocsContext> DocsOptions()
        => new DbContextOptionsBuilder<DocsContext>()
            .UseCamusDB(ConnectionString("rest"))
            .ReplaceService<IModelCacheKeyFactory, TableNameCacheKeyFactory>()
            .Options;

    private static async Task ExecuteDdlAsync(CamusConnection connection, string sql)
    {
        await using CamusCommand command = connection.CreateCamusCommand(sql);
        Assert.True(await command.ExecuteDDLAsync());
    }

    private static async Task<string> ShowCreateTableAsync(CamusConnection connection, string table)
    {
        await using CamusCommand command = connection.CreateCamusCommand($"SHOW CREATE TABLE {table}");
        await using CamusDataReader reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync(default));
        return reader.GetString(1);
    }

    private static async Task AssertRowAsync(
        CamusConnection connection,
        string table,
        string id,
        string title,
        string body,
        byte[] image,
        byte[] embedding,
        string[] tags)
    {
        await using (CamusCommand narrow = connection.CreateCamusCommand($"SELECT title FROM {table} WHERE id = @id"))
        {
            narrow.Parameters.Add("@id", ColumnType.Id, id);
            await using CamusDataReader reader = await narrow.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync(default));
            Assert.Equal(title, reader.GetString(0));
        }

        await using CamusCommand select = connection.CreateCamusCommand(
            $"SELECT title, body, image, embedding, tags FROM {table} WHERE id = @id");
        select.Parameters.Add("@id", ColumnType.Id, id);

        await using CamusDataReader row = await select.ExecuteReaderAsync();
        Assert.True(await row.ReadAsync(default));

        Assert.Equal(title, row.GetString(row.GetOrdinal("title")));
        Assert.Equal(body, row.GetString(row.GetOrdinal("body")));
        Assert.Equal(image, row.GetFieldValue<byte[]>(row.GetOrdinal("image")));
        Assert.Equal(embedding, row.GetFieldValue<byte[]>(row.GetOrdinal("embedding")));
        Assert.Equal(tags, ((object?[])row.GetValue(row.GetOrdinal("tags"))).Cast<string>().ToArray());
    }

    private static byte[] RandomBytes(int length, int seed)
    {
        byte[] bytes = new byte[length];
        new Random(seed).NextBytes(bytes);
        return bytes;
    }

    private sealed class TableNameCacheKeyFactory : IModelCacheKeyFactory
    {
        public object Create(DbContext context, bool designTime)
            => context is DocsContext docs ? (docs.Table, designTime) : (object)(context.GetType(), designTime);
    }

    private sealed class DocsContext(DbContextOptions options, string table) : DbContext(options)
    {
        public string Table { get; } = table;

        public DbSet<Doc> Docs => Set<Doc>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Doc>(b =>
            {
                b.ToTable(Table);
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id");
                b.Property(e => e.Body).HasStorage(CamusColumnStorage.Extended);
                b.Property(e => e.Image).HasStorage(CamusColumnStorage.External);
                b.Property(e => e.Embedding).HasMaxLength(3072).HasStorage(CamusColumnStorage.Plain);
            });
        }
    }

    private sealed class Doc
    {
        public string Id { get; set; } = "";
        public string Title { get; set; } = "";
        public string Body { get; set; } = "";
        public byte[]? Image { get; set; }
        public byte[]? Embedding { get; set; }
    }
}
