using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace CamusDB.EntityFrameworkCore;

public class CamusMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public CamusMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
        : base(dependencies) { }

    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        var pkCols = operation.PrimaryKey?.Columns.ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];

        builder.Append("CREATE TABLE ").Append(operation.Name).AppendLine(" (");

        bool first = true;
        foreach (var col in operation.Columns)
        {
            if (!first) builder.AppendLine(",");
            first = false;

            builder.Append(col.Name).Append(" ").Append(GetDdlType(col));

            if (pkCols.Contains(col.Name))
                builder.Append(" PRIMARY KEY NOT NULL");
            else if (!col.IsNullable)
                builder.Append(" NOT NULL");
        }

        builder.AppendLine().Append(")");

        if (terminate)
            builder.EndCommand();
    }

    protected override void Generate(
        DropTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder.Append("DROP TABLE ").Append(operation.Name);

        if (terminate)
            builder.EndCommand();
    }

    protected override void Generate(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder.Append(operation.IsUnique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ")
               .Append(operation.Name)
               .Append(" ON ")
               .Append(operation.Table)
               .Append(" (")
               .Append(string.Join(", ", operation.Columns))
               .Append(")");

        if (terminate)
            builder.EndCommand();
    }

    protected override void Generate(
        DropIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder.Append("DROP INDEX ").Append(operation.Name)
               .Append(" ON ").Append(operation.Table ?? "");

        if (terminate)
            builder.EndCommand();
    }

    protected override void Generate(
        SqlOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder)
    {
        builder.AppendLines(operation.Sql);
        builder.EndCommand(operation.SuppressTransaction);
    }

    // Schema operations — CamusDB has no schema concept
    protected override void Generate(EnsureSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(DropSchemaOperation operation, IModel? model, MigrationCommandListBuilder builder) { }

    // Unsupported structural changes
    protected override void Generate(AddColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support ALTER TABLE ADD COLUMN via migrations.");

    protected override void Generate(DropColumnOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support ALTER TABLE DROP COLUMN via migrations.");

    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support ALTER TABLE ALTER COLUMN via migrations.");

    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support RENAME COLUMN via migrations.");

    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support RENAME TABLE via migrations.");

    protected override void Generate(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support foreign key constraints.");

    protected override void Generate(DropForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support foreign key constraints.");

    protected override void Generate(AddPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support ADD PRIMARY KEY via migrations.");

    protected override void Generate(DropPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support DROP PRIMARY KEY via migrations.");

    protected override void Generate(AddUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support UNIQUE constraints via migrations.");

    protected override void Generate(DropUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support UNIQUE constraints via migrations.");

    protected override void Generate(AddCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support CHECK constraints.");

    protected override void Generate(DropCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support CHECK constraints.");

    protected override void Generate(CreateSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support sequences.");

    protected override void Generate(DropSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support sequences.");

    protected override void Generate(AlterSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support sequences.");

    protected override void Generate(RenameSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support sequences.");

    protected override void Generate(RestartSequenceOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support sequences.");

    protected override void Generate(AlterDatabaseOperation operation, IModel? model, MigrationCommandListBuilder builder) { }

    protected override void Generate(AlterTableOperation operation, IModel? model, MigrationCommandListBuilder builder) { }

    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support RENAME INDEX via migrations.");

    private static string GetDdlType(ColumnOperation col)
    {
        var storeType = (col.ColumnType ?? "").ToUpperInvariant();
        return storeType switch
        {
            "ID" or "OID" => "OID",
            "STRING" => "STRING",
            "BOOL" => "BOOL",
            "INT64" => "INT64",
            "FLOAT64" => "FLOAT64",
            _ => col.ClrType == typeof(bool) ? "BOOL"
                : col.ClrType == typeof(double) || col.ClrType == typeof(float) ? "FLOAT64"
                : col.ClrType == typeof(int) || col.ClrType == typeof(long) || col.ClrType == typeof(short) ? "INT64"
                : "STRING"
        };
    }
}
