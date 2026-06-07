using System.Globalization;
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
        var helper = Dependencies.SqlGenerationHelper;
        var pkCols = operation.PrimaryKey?.Columns.ToHashSet(StringComparer.OrdinalIgnoreCase) ?? [];

        builder.Append("CREATE TABLE ").Append(helper.DelimitIdentifier(operation.Name)).AppendLine(" (");

        bool first = true;
        foreach (var col in operation.Columns)
        {
            if (!first) builder.AppendLine(",");
            first = false;

            builder.Append(helper.DelimitIdentifier(col.Name)).Append(" ").Append(GetDdlType(col));

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
        builder.Append("DROP TABLE ").Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name));

        if (terminate)
            builder.EndCommand();
    }

    protected override void Generate(
        AddColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        var helper = Dependencies.SqlGenerationHelper;

        builder.Append("ALTER TABLE ").Append(helper.DelimitIdentifier(operation.Table))
               .Append(" ADD COLUMN ").Append(helper.DelimitIdentifier(operation.Name))
               .Append(" ").Append(GetDdlType(operation));

        if (!operation.IsNullable)
            builder.Append(" NOT NULL");

        if (operation.DefaultValue is not null)
            builder.Append(" DEFAULT (").Append(FormatDefaultValue(operation.DefaultValue)).Append(")");
        else if (!string.IsNullOrEmpty(operation.DefaultValueSql))
            builder.Append(" DEFAULT (").Append(operation.DefaultValueSql).Append(")");

        if (terminate)
            builder.EndCommand();
    }

    protected override void Generate(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        var helper = Dependencies.SqlGenerationHelper;

        builder.Append("ALTER TABLE ").Append(helper.DelimitIdentifier(operation.Table))
               .Append(" DROP COLUMN ").Append(helper.DelimitIdentifier(operation.Name));

        if (terminate)
            builder.EndCommand();
    }

    protected override void Generate(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        var helper = Dependencies.SqlGenerationHelper;

        // Both CREATE INDEX and CREATE UNIQUE INDEX are supported by CamusDB
        builder.Append(operation.IsUnique ? "CREATE UNIQUE INDEX " : "CREATE INDEX ")
               .Append(helper.DelimitIdentifier(operation.Name))
               .Append(" ON ")
               .Append(helper.DelimitIdentifier(operation.Table))
               .Append(" (")
               .Append(string.Join(", ", operation.Columns.Select(c => helper.DelimitIdentifier(c))))
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
        var helper = Dependencies.SqlGenerationHelper;

        // CamusDB syntax: ALTER TABLE t DROP INDEX name
        builder.Append("ALTER TABLE ").Append(helper.DelimitIdentifier(operation.Table ?? ""))
               .Append(" DROP INDEX ").Append(helper.DelimitIdentifier(operation.Name ?? ""));

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

    // No-ops for table/database metadata changes that don't affect CamusDB structure
    protected override void Generate(AlterDatabaseOperation operation, IModel? model, MigrationCommandListBuilder builder) { }
    protected override void Generate(AlterTableOperation operation, IModel? model, MigrationCommandListBuilder builder) { }

    // Unsupported operations
    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support altering an existing column type.");

    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support RENAME COLUMN.");

    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support RENAME TABLE.");

    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support RENAME INDEX.");

    protected override void Generate(AddForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support foreign key constraints.");

    protected override void Generate(DropForeignKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support foreign key constraints.");

    protected override void Generate(AddPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support ADD PRIMARY KEY via migrations.");

    protected override void Generate(DropPrimaryKeyOperation operation, IModel? model, MigrationCommandListBuilder builder, bool terminate = true)
        => throw new NotSupportedException("CamusDB does not support DROP PRIMARY KEY via migrations.");

    protected override void Generate(AddUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support inline UNIQUE constraints; use CreateIndex with IsUnique=true instead.");

    protected override void Generate(DropUniqueConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
        => throw new NotSupportedException("CamusDB does not support inline UNIQUE constraints.");

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

    private static string FormatDefaultValue(object value) => value switch
    {
        bool b   => b ? "true" : "false",
        string s => $"'{s}'",
        int i    => i.ToString(CultureInfo.InvariantCulture),
        long l   => l.ToString(CultureInfo.InvariantCulture),
        float f  => f.ToString(CultureInfo.InvariantCulture),
        double d => d.ToString(CultureInfo.InvariantCulture),
        _        => value.ToString() ?? "null"
    };
}
