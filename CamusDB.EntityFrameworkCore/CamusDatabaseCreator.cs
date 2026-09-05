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

        // The runtime-optimized model omits CHECK constraints (GetCheckConstraints throws on it);
        // the design-time model carries them, so use it for DDL generation.
        var model = _currentContext.Context.GetService<IDesignTimeModel>().Model;

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

    /// <summary>
    /// Composes the <c>CREATE TABLE</c> behind <c>EnsureCreated</c>.
    ///
    /// <para>Every name is delimited through <see cref="CamusIdentifier"/>, the same rule the migration
    /// generator applies. Appending a name raw would corrupt the statement for any legitimate name that
    /// is a reserved word or carries a space, and would let a model whose identifiers are derived at
    /// runtime (tenant provisioning, a dynamic schema) compose SQL nobody wrote. The CHECK expression
    /// stays verbatim — it is a SQL fragment the model author wrote, not a name.</para>
    /// </summary>
    private static string BuildCreateTableSql(IEntityType entityType, string tableName)
    {
        var sb = new StringBuilder();
        sb.Append("CREATE TABLE ").Append(CamusIdentifier.Delimit(tableName, nameof(tableName))).Append(" (");

        var pk = entityType.FindPrimaryKey();
        var pkProps = pk?.Properties.ToHashSet() ?? [];
        // A property always resolves to a column name here (these are mapped scalar properties); the
        // fallback keeps a null out of the composed DDL rather than emitting an empty identifier.
        var pkColumns = pk?.Properties.Select(p => p.GetColumnName() ?? p.Name).ToList() ?? [];

        bool first = true;
        foreach (var prop in entityType.GetProperties())
        {
            if (!first) sb.Append(", ");
            first = false;

            var columnName = prop.GetColumnName() ?? prop.Name;
            var ddlType = GetDdlType(prop, pkProps.Contains(prop));

            sb.Append(CamusIdentifier.Delimit(columnName, "columnName")).Append(' ').Append(ddlType);

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
            sb.Append(string.Join(", ", pkColumns.Select(c => CamusIdentifier.Delimit(c, "primaryKeyColumn"))));
            sb.Append(')');
        }

        // Named CHECK constraints declared via ToTable(t => t.HasCheckConstraint(...)).
        foreach (var check in entityType.GetCheckConstraints())
        {
            // A check constraint is always named by the time it reaches the model; the throw states that
            // rather than letting a null reach the composed DDL as an empty constraint name.
            string checkName = check.Name
                ?? throw new InvalidOperationException($"The check constraint on '{tableName}' has no name.");

            sb.Append(", CONSTRAINT ").Append(CamusIdentifier.Delimit(checkName, "checkConstraintName"))
              .Append(" CHECK (").Append(check.Sql).Append(')');
        }

        sb.Append(')');
        return sb.ToString();
    }

    private static string GetDdlType(IProperty property, bool isPrimaryKey)
    {
        var storeType = (property.GetColumnType() ?? "").ToUpperInvariant();
        var clrType = Nullable.GetUnderlyingType(property.ClrType) ?? property.ClrType;

        // Native ARRAY(T) columns arrive as "array(int64)" etc.; render as "ARRAY(INT64)".
        if (storeType.StartsWith("ARRAY(", StringComparison.Ordinal))
            return storeType;

        // An explicitly sized BYTES(N) — the usual way to declare an embedding column — carries its own
        // width already. Pass it through rather than fall to the CLR-type arm, which would drop the size.
        if (storeType.StartsWith("BYTES(", StringComparison.Ordinal))
            return storeType;

        return storeType switch
        {
            "ID" or "OID"             => "OID",
            "UUID" or "GUID"          => "UUID",
            "STRING"                  => StringDdl(property),
            "BOOL"                    => "BOOL",
            "INT64"                   => "INT64",
            "FLOAT64"                 => "FLOAT64",
            "FLOAT32" or "REAL"       => "FLOAT32",
            "BYTES" or "BLOB"         => BytesDdl(property),
            "DATE"                    => "DATE",
            "DATETIME" or "TIMESTAMP" => "DATETIME",
            _ => clrType == typeof(bool) ? "BOOL"
                : clrType == typeof(float) ? "FLOAT32"
                : clrType == typeof(double) ? "FLOAT64"
                : clrType == typeof(int) || clrType == typeof(long) || clrType == typeof(short) ? "INT64"
                : clrType == typeof(byte[]) ? BytesDdl(property)
                : clrType == typeof(DateOnly) ? "DATE"
                : clrType == typeof(DateTime) || clrType == typeof(DateTimeOffset) ? "DATETIME"
                : StringDdl(property)
        };
    }

    private static string StringDdl(IProperty property)
        => property.GetMaxLength() is int n and > 0 ? $"STRING({n})" : "STRING";

    /// <summary>
    /// <c>BYTES(N)</c> from <c>HasMaxLength(n)</c>, where <c>N</c> is a maximum byte count rather than a
    /// fixed width. A vector column is declared this way — a 768-element float32 embedding is
    /// <c>bytes(3072)</c> — so dropping the size would leave every embedding column at the server's
    /// default maximum. The size does not pin a dimension; a <c>CHECK (vector_dims(c) = 768)</c> does.
    /// </summary>
    private static string BytesDdl(IProperty property)
        => property.GetMaxLength() is int n and > 0 ? $"BYTES({n})" : "BYTES";
}
