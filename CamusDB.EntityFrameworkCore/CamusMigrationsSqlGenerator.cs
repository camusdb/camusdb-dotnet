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
        var pkCols = operation.PrimaryKey?.Columns ?? [];

        builder.Append("CREATE TABLE IF NOT EXISTS ").Append(helper.DelimitIdentifier(operation.Name)).AppendLine(" (");

        bool firstItem = true;

        foreach (var col in operation.Columns)
        {
            if (!firstItem)
                builder.AppendLine(",");
            firstItem = false;

            builder.Append(helper.DelimitIdentifier(col.Name)).Append(" ").Append(GetDdlType(col));

            if (!col.IsNullable)
                builder.Append(" NOT NULL");
        }

        if (pkCols.Length > 0)
        {
            builder.AppendLine(",")
                   .Append("PRIMARY KEY (")
                   .Append(string.Join(", ", pkCols.Select(c => helper.DelimitIdentifier(c))))
                   .Append(")");
        }

        // Table-level, named CHECK constraints. CamusDB desugars column-level checks into named
        // table-level constraints internally, so emitting them all at the table level is faithful.
        foreach (var check in operation.CheckConstraints)
        {
            builder.AppendLine(",")
                   .Append("CONSTRAINT ").Append(helper.DelimitIdentifier(check.Name))
                   .Append(" CHECK (").Append(check.Sql).Append(")");
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
        builder.Append(operation.IsUnique ? "CREATE UNIQUE INDEX IF NOT EXISTS " : "CREATE INDEX IF NOT EXISTS ")
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
        InsertDataOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        var helper = Dependencies.SqlGenerationHelper;
        var tableName = helper.DelimitIdentifier(operation.Table, operation.Schema);
        var columnList = string.Join(", ", operation.Columns.Select(c => helper.DelimitIdentifier(c)));
        var rows = operation.Values.GetLength(0);
        var cols = operation.Values.GetLength(1);

        for (int row = 0; row < rows; row++)
        {
            var literals = new string[cols];
            for (int col = 0; col < cols; col++)
                literals[col] = FormatLiteral(operation.Values[row, col]);

            builder
                .Append("INSERT INTO ").Append(tableName)
                .Append(" (").Append(columnList).Append(")")
                .Append(" VALUES (")
                .Append(string.Join(", ", literals))
                .Append(")");

            if (terminate)
                builder.EndCommand();
        }
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

    // CamusDB cannot change a column's stored type in place, but it does support toggling a column's
    // NOT NULL constraint (ALTER COLUMN ... SET/DROP NOT NULL). Map a nullability-only change to that;
    // reject anything that would require rewriting the column's type.
    protected override void Generate(AlterColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        var helper = Dependencies.SqlGenerationHelper;

        if (!string.Equals(GetDdlType(operation), GetDdlType(operation.OldColumn), StringComparison.Ordinal))
            throw new NotSupportedException("CamusDB does not support altering an existing column type.");

        if (operation.IsNullable == operation.OldColumn.IsNullable)
            throw new NotSupportedException("CamusDB only supports altering a column's nullability.");

        builder.Append("ALTER TABLE ").Append(helper.DelimitIdentifier(operation.Table))
               .Append(" ALTER COLUMN ").Append(helper.DelimitIdentifier(operation.Name))
               .Append(operation.IsNullable ? " DROP NOT NULL" : " SET NOT NULL");
        builder.EndCommand();
    }

    protected override void Generate(RenameColumnOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        var helper = Dependencies.SqlGenerationHelper;
        builder.Append("ALTER TABLE ").Append(helper.DelimitIdentifier(operation.Table))
               .Append(" RENAME COLUMN ").Append(helper.DelimitIdentifier(operation.Name))
               .Append(" TO ").Append(helper.DelimitIdentifier(operation.NewName));
        builder.EndCommand();
    }

    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        var helper = Dependencies.SqlGenerationHelper;
        var newName = operation.NewName ?? throw new InvalidOperationException("RenameTableOperation.NewName is required.");
        builder.Append("ALTER TABLE ").Append(helper.DelimitIdentifier(operation.Name))
               .Append(" RENAME TO ").Append(helper.DelimitIdentifier(newName));
        builder.EndCommand();
    }

    protected override void Generate(RenameIndexOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        var helper = Dependencies.SqlGenerationHelper;
        var newName = operation.NewName ?? throw new InvalidOperationException("RenameIndexOperation.NewName is required.");
        builder.Append("ALTER TABLE ").Append(helper.DelimitIdentifier(operation.Table ?? ""))
               .Append(" RENAME INDEX ").Append(helper.DelimitIdentifier(operation.Name))
               .Append(" TO ").Append(helper.DelimitIdentifier(newName));
        builder.EndCommand();
    }

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
    {
        var helper = Dependencies.SqlGenerationHelper;
        builder.Append("ALTER TABLE ").Append(helper.DelimitIdentifier(operation.Table))
               .Append(" ADD CONSTRAINT ").Append(helper.DelimitIdentifier(operation.Name))
               .Append(" CHECK (").Append(operation.Sql).Append(")");
        builder.EndCommand();
    }

    // CamusDB resolves DROP CONSTRAINT against both CHECK and named NOT NULL constraints by name.
    protected override void Generate(DropCheckConstraintOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        var helper = Dependencies.SqlGenerationHelper;
        builder.Append("ALTER TABLE ").Append(helper.DelimitIdentifier(operation.Table))
               .Append(" DROP CONSTRAINT ").Append(helper.DelimitIdentifier(operation.Name));
        builder.EndCommand();
    }

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

        // Native ARRAY(T) columns arrive as "array(int64)" etc.; render as "ARRAY(INT64)".
        if (storeType.StartsWith("ARRAY(", StringComparison.Ordinal))
            return storeType;

        return storeType switch
        {
            "ID" or "OID"          => "OID",
            "UUID" or "GUID"       => "UUID",
            "STRING"               => StringDdl(col),
            "BOOL"                 => "BOOL",
            "INT64"                => "INT64",
            "FLOAT64"              => "FLOAT64",
            "FLOAT32" or "REAL"    => "FLOAT32",
            "BYTES" or "BLOB"      => "BYTES",
            "DATE"                 => "DATE",
            "DATETIME" or "TIMESTAMP" => "DATETIME",
            _ => col.ClrType == typeof(bool) ? "BOOL"
                : col.ClrType == typeof(float) ? "FLOAT32"
                : col.ClrType == typeof(double) ? "FLOAT64"
                : col.ClrType == typeof(int) || col.ClrType == typeof(long) || col.ClrType == typeof(short) ? "INT64"
                : col.ClrType == typeof(byte[]) ? "BYTES"
                : col.ClrType == typeof(DateOnly) ? "DATE"
                : col.ClrType == typeof(DateTime) || col.ClrType == typeof(DateTimeOffset) ? "DATETIME"
                : StringDdl(col)
        };
    }

    private static string StringDdl(ColumnOperation col)
        => col.MaxLength is int n and > 0 ? $"STRING({n.ToString(CultureInfo.InvariantCulture)})" : "STRING";

    private static string FormatDefaultValue(object value) => value switch
    {
        bool b           => b ? "true" : "false",
        string s         => $"'{s.Replace("'", "''")}'",
        int i            => i.ToString(CultureInfo.InvariantCulture),
        long l           => l.ToString(CultureInfo.InvariantCulture),
        float f          => f.ToString(CultureInfo.InvariantCulture),
        double d         => d.ToString(CultureInfo.InvariantCulture),
        byte[] bytes     => ToHexLiteral(bytes),
        DateOnly d       => $"'{d:yyyy-MM-dd}'",
        DateTime dt      => $"'{ToIso(dt)}'",
        DateTimeOffset o => $"'{o.UtcDateTime:yyyy-MM-ddTHH:mm:ss.fffffffZ}'",
        _                => value.ToString() ?? "null"
    };

    private static string FormatLiteral(object? value) => value switch
    {
        null or DBNull => "NULL",
        bool b         => b ? "true" : "false",
        short s        => s.ToString(CultureInfo.InvariantCulture),
        int i          => i.ToString(CultureInfo.InvariantCulture),
        long l         => l.ToString(CultureInfo.InvariantCulture),
        float f        => f.ToString(CultureInfo.InvariantCulture),
        double d       => d.ToString(CultureInfo.InvariantCulture),
        byte[] bytes   => ToHexLiteral(bytes),
        DateOnly d     => $"'{d:yyyy-MM-dd}'",
        DateTime dt    => $"'{ToIso(dt)}'",
        DateTimeOffset o => $"'{o.UtcDateTime:yyyy-MM-ddTHH:mm:ss.fffffffZ}'",
        Guid g         => $"'{g}'",
        string s       => $"'{s.Replace("'", "''")}'",
        _              => $"'{value.ToString()?.Replace("'", "''") ?? ""}'"
    };

    // SQL bytes literals are 0x-prefixed hex (the JSON path uses base64 instead).
    private static string ToHexLiteral(byte[] bytes) => "0x" + Convert.ToHexString(bytes);

    private static string ToIso(DateTime dt)
    {
        DateTime utc = dt.Kind switch
        {
            DateTimeKind.Utc => dt,
            DateTimeKind.Local => dt.ToUniversalTime(),
            _ => DateTime.SpecifyKind(dt, DateTimeKind.Utc)
        };
        return utc.ToString("yyyy-MM-ddTHH:mm:ss.fffffffZ", CultureInfo.InvariantCulture);
    }
}
