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

    // CamusDB server is assumed to be running — existence checks are not meaningful
    public override bool Exists() => true;

    public override Task<bool> ExistsAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(true);

    // CamusDB databases are logical — no explicit creation is needed
    public override void Create() { }

    public override Task CreateAsync(CancellationToken cancellationToken = default)
        => Task.CompletedTask;

    public override void Delete()
        => throw new NotSupportedException("CamusDB does not support dropping databases via the EF provider.");

    public override Task DeleteAsync(CancellationToken cancellationToken = default)
        => Task.FromException(new NotSupportedException("CamusDB does not support dropping databases via the EF provider."));

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

        bool first = true;
        foreach (var prop in entityType.GetProperties())
        {
            if (!first) sb.Append(", ");
            first = false;

            var columnName = prop.GetColumnName();
            var ddlType = GetDdlType(prop, pkProps.Contains(prop));

            sb.Append(columnName).Append(' ').Append(ddlType);

            if (pkProps.Contains(prop))
                sb.Append(" PRIMARY KEY NOT NULL");
            else if (!prop.IsNullable)
                sb.Append(" NOT NULL");
        }

        sb.Append(')');
        return sb.ToString();
    }

    private static string GetDdlType(IProperty property, bool isPrimaryKey)
    {
        var storeType = property.GetColumnType() ?? "";
        return storeType.ToUpperInvariant() switch
        {
            "ID" or "OID" => "OID",
            "STRING" => "STRING",
            "BOOL" => "BOOL",
            "INT64" => "INT64",
            "FLOAT64" => "FLOAT64",
            _ => "STRING"
        };
    }
}
