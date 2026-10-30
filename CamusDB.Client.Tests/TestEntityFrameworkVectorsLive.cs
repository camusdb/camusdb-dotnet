/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace CamusDB.Client.Tests;

/// <summary>
/// IsVector against a live server: the DDL that EF generates, a KNN query over a float[] vector that is
/// not loaded with its entity, an explicit projection of it, and the save rules that keep a vector that
/// was not loaded. Set <c>CAMUSDB_TEST_VECTORS=true</c> to run.
/// </summary>
public class TestEntityFrameworkVectorsLive
{
    private static bool Enabled
        => string.Equals(Environment.GetEnvironmentVariable("CAMUSDB_TEST_VECTORS"), "true", StringComparison.OrdinalIgnoreCase);

    private static string ConnString(string protocol) => protocol == "grpc"
        ? $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_GRPC_ENDPOINT") ?? "http://localhost:5096"};Database=test;Protocol=grpc"
        : $"Endpoint={Environment.GetEnvironmentVariable("CAMUSDB_TEST_ENDPOINT") ?? "http://localhost:5095"};Database=test";

    // One fixed table: EF Core builds one model for each context type.
    private const string Table = "vec_live_chunks";

    private static ChunkContext NewContext(string protocol)
        => new(new DbContextOptionsBuilder<ChunkContext>().UseCamusDB(ConnString(protocol)).Options);

    /// <summary>Drops the table and creates it again from the DDL that the model generates.</summary>
    private static async Task CreateTableAsync(string protocol)
    {
        string script;
        await using (ChunkContext ctx = NewContext(protocol))
            script = ctx.Database.GenerateCreateScript().Trim().TrimEnd(';');

        await using CamusConnection connection = new(new CamusConnectionStringBuilder(ConnString(protocol)));
        await connection.OpenAsync();
        await connection.CreateDatabaseAsync("test", ifNotExists: true);

        await using (CamusCommand drop = connection.CreateCamusCommand($"DROP TABLE IF EXISTS {Table}"))
            await drop.ExecuteDDLAsync();

        await using CamusCommand create = connection.CreateCamusCommand(script);
        await create.ExecuteDDLAsync();
    }

    [Theory]
    [InlineData("rest")]
    [InlineData("grpc")]
    public async Task VectorThatIsNotLoadedWithItsEntity(string protocol)
    {
        if (!Enabled)
            return;

        await CreateTableAsync(protocol);

        await using (ChunkContext ctx = NewContext(protocol))
        {
            ctx.Chunks.Add(new Chunk { Id = "a", Content = "alpha", Embedding = [1, 0, 0, 0], Summary = [1, 1, 1, 1] });
            ctx.Chunks.Add(new Chunk { Id = "b", Content = "beta", Embedding = [0, 1, 0, 0] });
            ctx.Chunks.Add(new Chunk { Id = "c", Content = "gamma", Embedding = [0.9f, 0.1f, 0, 0] });
            await ctx.SaveChangesAsync();
        }

        // An entity query does not read the vector; a vector that is loaded with its entity is read.
        await using (ChunkContext ctx = NewContext(protocol))
        {
            List<Chunk> all = await ctx.Chunks.OrderBy(c => c.Id).ToListAsync();

            Assert.Equal(["a", "b", "c"], all.Select(c => c.Id));
            Assert.All(all, c => Assert.Null(c.Embedding));
            AssertVector(new float[] { 1, 1, 1, 1 }, all[0].Summary);
            Assert.Null(all[1].Summary);
        }

        // The KNN query ranks on the stored vector, and the query vector goes as bytes.
        await using (ChunkContext ctx = NewContext(protocol))
        {
            float[] q = [1, 0, 0, 0];

            List<Chunk> nearest = await ctx.Chunks
                .OrderBy(c => EF.Functions.CosineDistance(c.Embedding, q))
                .Take(2)
                .ToListAsync();

            Assert.Equal(["a", "c"], nearest.Select(c => c.Id));

            var withDistance = await ctx.Chunks
                .Select(c => new { c.Id, Distance = EF.Functions.L2Distance(c.Embedding, q) })
                .OrderBy(r => r.Distance)
                .FirstAsync();

            Assert.Equal("a", withDistance.Id);
            Assert.Equal(0.0, withDistance.Distance, 6);
        }

        // A projection that names the vector reads it.
        AssertVector(new float[] { 1, 0, 0, 0 }, await EmbeddingAsync(protocol, "a"));

        // A tracked entity saves its other columns and keeps the stored vector.
        await using (ChunkContext ctx = NewContext(protocol))
        {
            Chunk a = await ctx.Chunks.SingleAsync(c => c.Id == "a");
            a.Content = "alpha 2";
            await ctx.SaveChangesAsync();
        }

        AssertVector(new float[] { 1, 0, 0, 0 }, await EmbeddingAsync(protocol, "a"));
        Assert.Equal("alpha 2", await ContentAsync(protocol, "a"));

        // Update of a detached entity marks every property, but a null vector is not written.
        await using (ChunkContext ctx = NewContext(protocol))
        {
            ctx.Update(new Chunk { Id = "b", Content = "beta 2", Embedding = null! });
            await ctx.SaveChangesAsync();
        }

        AssertVector(new float[] { 0, 1, 0, 0 }, await EmbeddingAsync(protocol, "b"));
        Assert.Equal("beta 2", await ContentAsync(protocol, "b"));

        // A vector that the entity sets is written.
        await using (ChunkContext ctx = NewContext(protocol))
        {
            ctx.Update(new Chunk { Id = "b", Content = "beta 3", Embedding = [0, 0, 1, 0] });
            await ctx.SaveChangesAsync();
        }

        AssertVector(new float[] { 0, 0, 1, 0 }, await EmbeddingAsync(protocol, "b"));

        // A loaded float[] vector changed in place is saved.
        await using (ChunkContext ctx = NewContext(protocol))
        {
            Chunk a = await ctx.Chunks.SingleAsync(c => c.Id == "a");
            a.Summary![0] = 5;
            await ctx.SaveChangesAsync();
        }

        await using (ChunkContext ctx = NewContext(protocol))
        {
            Chunk a = await ctx.Chunks.SingleAsync(c => c.Id == "a");
            AssertVector(new float[] { 5, 1, 1, 1 }, a.Summary);
        }

        // The dimension CHECK refuses a short vector.
        await using (ChunkContext ctx = NewContext(protocol))
        {
            ctx.Chunks.Add(new Chunk { Id = "d", Content = "delta", Embedding = [1, 2, 3] });
            await Assert.ThrowsAsync<DbUpdateException>(() => ctx.SaveChangesAsync());
        }
    }

    private static void AssertVector(float[] expected, float[]? actual)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected, actual);
    }

    private static async Task<float[]> EmbeddingAsync(string protocol, string id)
    {
        await using ChunkContext ctx = NewContext(protocol);
        return await ctx.Chunks.Where(c => c.Id == id).Select(c => c.Embedding).SingleAsync();
    }

    private static async Task<string> ContentAsync(string protocol, string id)
    {
        await using ChunkContext ctx = NewContext(protocol);
        return await ctx.Chunks.Where(c => c.Id == id).Select(c => c.Content).SingleAsync();
    }

    private sealed class ChunkContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Chunk> Chunks => Set<Chunk>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Chunk>(b =>
            {
                b.ToTable(Table);
                b.HasKey(c => c.Id);
                b.Property(c => c.Id).HasColumnName("id");
                b.Property(c => c.Content).HasColumnName("content");
                b.Property(c => c.Embedding).HasColumnName("embedding").IsRequired().IsVector(4, loadWithEntity: false);
                b.Property(c => c.Summary).HasColumnName("summary").IsVector(4);
            });
    }

    private sealed class Chunk
    {
        public string Id { get; set; } = "";
        public string Content { get; set; } = "";
        public float[] Embedding { get; set; } = [];
        public float[]? Summary { get; set; }
    }
}
