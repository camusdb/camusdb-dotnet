using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

public class CamusDatabaseCreator : RelationalDatabaseCreator
{
    private readonly CamusRelationalConnection _connection;
    private readonly ICurrentDbContext _currentContext;

    public CamusDatabaseCreator(
        RelationalDatabaseCreatorDependencies dependencies,
        IRelationalConnection connection,
        ICurrentDbContext currentContext)
        : base(dependencies)
    {
        _connection = (CamusRelationalConnection)connection;
        _currentContext = currentContext;
    }

    // Return false so EnsureCreated always calls Create(), which uses IF NOT EXISTS and is idempotent.
    public override bool Exists() => false;

    public override Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(false);

    public override void Create()
        => CreateAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task CreateAsync(CancellationToken cancellationToken = default)
    {
        var camusConn = _connection.DbConnection;
        await camusConn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await camusConn.CreateDatabaseAsync(ifNotExists: true, cancellationToken).ConfigureAwait(false);
    }

    public override void Delete()
        => DeleteAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task DeleteAsync(CancellationToken cancellationToken = default)
    {
        var camusConn = _connection.DbConnection;
        await camusConn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await camusConn.DropDatabaseAsync(cancellationToken).ConfigureAwait(false);
    }

    // Without a metadata query we conservatively report no tables so EnsureCreated always tries to create
    public override bool HasTables() => false;

    public override Task<bool> HasTablesAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(false);

    public override void CreateTables()
        => CreateTablesAsync(CancellationToken.None).GetAwaiter().GetResult();

    public override async Task CreateTablesAsync(CancellationToken cancellationToken = default)
    {
        var camusConn = _connection.DbConnection;
        await camusConn.OpenAsync(cancellationToken).ConfigureAwait(false);

        var model = _currentContext.Context.Model;

        foreach (var entityType in model.GetEntityTypes())
        {
            var tableName = entityType.GetTableName();
            if (tableName is null)
                continue;

            var ddl = BuildCreateTableSql(entityType, tableName);
            var cmd = camusConn.CreateCamusCommand(ddl);

            try
            {
                await cmd.ExecuteDDLAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (CamusDB.Client.CamusException ex) when (IsTableAlreadyExistsError(ex))
            {
                // Table already exists — safe to continue
            }
        }
    }

    private static bool IsTableAlreadyExistsError(CamusDB.Client.CamusException ex)
        => ex.Message.Contains("already exists", StringComparison.OrdinalIgnoreCase);

    private static string BuildCreateTableSql(IEntityType entityType, string tableName)
    {
        var sb = new StringBuilder();
        sb.Append("CREATE TABLE ").Append(tableName).Append(" (");

        var pk = entityType.FindPrimaryKey();
        var pkProps = pk?.Properties.ToHashSet() ?? [];
        var pkColumns = pk?.Properties.Select(p => p.GetColumnName()).ToList() ?? [];

        bool first = true;
        foreach (var prop in entityType.GetProperties())
        {
            if (!first) sb.Append(", ");
            first = false;

            var columnName = prop.GetColumnName();
            var ddlType = GetDdlType(prop, pkProps.Contains(prop));

            sb.Append(columnName).Append(' ').Append(ddlType);

            if (pkProps.Contains(prop) || !prop.IsNullable)
                sb.Append(" NOT NULL");
        }

        // Emit a single table-level PRIMARY KEY constraint so that composite keys
        // (e.g. HasKey(e => new { e.UsersId, e.Id })) are declared correctly.
        // Column-level "PRIMARY KEY NOT NULL" on each individual column would create
        // separate single-column PK constraints and only the last one would survive.
        if (pkColumns.Count > 0)
        {
            sb.Append(", PRIMARY KEY (");
            sb.Append(string.Join(", ", pkColumns));
            sb.Append(')');
        }

        sb.Append(')');
        return sb.ToString();
    }

    private static string GetDdlType(IProperty property, bool isPrimaryKey)
    {
        var storeType = (property.GetColumnType() ?? "").ToUpperInvariant();
        var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;

        return storeType switch
        {
            "ID" or "OID"             => "OID",
            "UUID" or "GUID"          => "UUID",
            "STRING"                  => StringDdl(property),
            "BOOL"                    => "BOOL",
            "INT64"                   => "INT64",
            "FLOAT64"                 => "FLOAT64",
            "FLOAT32" or "REAL"       => "FLOAT32",
            "BYTES" or "BLOB"         => "BYTES",
            "DATE"                    => "DATE",
            "DATETIME" or "TIMESTAMP" => "DATETIME",
            _ => clrType == typeof(bool) ? "BOOL"
                : clrType == typeof(float) ? "FLOAT32"
                : clrType == typeof(double) ? "FLOAT64"
                : clrType == typeof(int) || clrType == typeof(long) || clrType == typeof(short) ? "INT64"
                : clrType == typeof(byte[]) ? "BYTES"
                : clrType == typeof(DateOnly) ? "DATE"
                : clrType == typeof(DateTime) || clrType == typeof(DateTimeOffset) ? "DATETIME"
                : StringDdl(property)
        };
    }

    private static string StringDdl(IProperty property)
        => property.GetMaxLength() is int n and > 0 ? $"STRING({n})" : "STRING";
}
