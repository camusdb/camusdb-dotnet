/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CamusDB.Client.Tests;

/// <summary>
/// The <c>CREATE TABLE</c> that <c>EnsureCreated</c> composes. These tests read the composed text
/// only, so no server is needed.
/// </summary>
public class TestEntityFrameworkEnsureCreatedDdl
{
    private const string ConnString = "Endpoint=http://localhost:5095;Database=test";

    /// <summary>
    /// A column that declares no default must carry no <c>DEFAULT</c> clause.
    ///
    /// <para><c>IProperty.GetDefaultValue</c> answers with the CLR default of the type for a
    /// non-nullable value-type property that has no configured default. A <c>DATETIME</c> column then
    /// got <c>DEFAULT ('0001-01-01T00:00:00.0000000Z')</c>, which the server refuses with
    /// <c>Function 'coerce' cannot parse …</c>, and every <c>INT64</c>, <c>FLOAT</c> and <c>BOOL</c>
    /// column got a default the model never declared. <c>TryGetDefaultValue</c> is the test that
    /// separates the two cases.</para>
    /// </summary>
    [Fact]
    public void TestColumnsWithoutADefaultEmitNoDefaultClause()
    {
        string ddl = CreateTableSql<PlainContext>("ef_plain");

        Assert.DoesNotContain("DEFAULT", ddl);
        Assert.Contains("`Happened` DATETIME NOT NULL", ddl);
        Assert.Contains("`Day` DATE NOT NULL", ddl);
        Assert.Contains("`Score` FLOAT32 NOT NULL", ddl);
        Assert.Contains("`Count` INT64 NOT NULL", ddl);
        Assert.Contains("`Enabled` BOOL NOT NULL", ddl);
    }

    /// <summary>
    /// A default the model does declare still reaches the DDL, for both a value type and a string.
    /// </summary>
    [Fact]
    public void TestConfiguredDefaultsStillReachTheDdl()
    {
        string ddl = CreateTableSql<DefaultedContext>("ef_defaulted");

        Assert.Contains("`Score` FLOAT32 NOT NULL DEFAULT (3.5)", ddl);
        Assert.Contains("`Count` INT64 NOT NULL DEFAULT (7)", ddl);
        Assert.Contains("`Enabled` BOOL NOT NULL DEFAULT (true)", ddl);
        Assert.Contains("`Name` STRING(64) NOT NULL DEFAULT ('unnamed')", ddl);
        Assert.Contains("`Happened` DATETIME NOT NULL DEFAULT ('2026-05-01T08:00:00.0000000Z')", ddl);
    }

    private static string CreateTableSql<TContext>(string tableName) where TContext : DbContext
    {
        DbContextOptions<TContext> options = new DbContextOptionsBuilder<TContext>()
            .UseCamusDB(ConnString).Options;

        using TContext ctx = (TContext)Activator.CreateInstance(typeof(TContext), options)!;
        IModel model = ctx.GetService<IDesignTimeModel>().Model;
        IEntityType entityType = Assert.Single(model.GetEntityTypes());

        return CamusDatabaseCreator.BuildCreateTableSql(entityType, tableName);
    }

    public class Row
    {
        public string Id { get; set; } = "";

        public string Name { get; set; } = "";

        public float Score { get; set; }

        public long Count { get; set; }

        public bool Enabled { get; set; }

        public DateTime Happened { get; set; }

        public DateOnly Day { get; set; }
    }

    private class PlainContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Row> Rows => Set<Row>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Row>(b =>
            {
                b.ToTable("ef_plain");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasMaxLength(64);
            });
        }
    }

    private class DefaultedContext(DbContextOptions options) : DbContext(options)
    {
        public DbSet<Row> Rows => Set<Row>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Row>(b =>
            {
                b.ToTable("ef_defaulted");
                b.HasKey(e => e.Id);
                b.Property(e => e.Id).HasColumnType("id").ValueGeneratedOnAdd();
                b.Property(e => e.Name).HasMaxLength(64).HasDefaultValue("unnamed");
                b.Property(e => e.Score).HasDefaultValue(3.5f);
                b.Property(e => e.Count).HasDefaultValue(7L);
                b.Property(e => e.Enabled).HasDefaultValue(true);
                b.Property(e => e.Happened)
                 .HasDefaultValue(new DateTime(2026, 5, 1, 8, 0, 0, DateTimeKind.Utc));
            });
        }
    }
}
