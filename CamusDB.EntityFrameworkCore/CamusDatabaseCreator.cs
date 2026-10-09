using System.Text;
using CamusDB.Client;
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

        // Sequences first: a column default can draw from one, and the server checks that the
        // sequence exists when it creates the table. IF NOT EXISTS keeps the step idempotent, like
        // the tables below.
        foreach (var sequence in model.GetSequences())
        {
            var cmd = camusConn.CreateCamusCommand(CamusSequenceSyntax.Create(sequence, ifNotExists: true));
            await cmd.ExecuteDDLAsync(cancellationToken).ConfigureAwait(false);
        }

        bool foreignKeys = CamusForeignKeySyntax.IsEnabled(_currentContext.Context);

        foreach (var entityType in OrderForCreation(model.GetEntityTypes(), foreignKeys))
        {
            var tableName = entityType.GetTableName();
            if (tableName is null)
                continue;

            var ddl = BuildCreateTableSql(entityType, tableName, foreignKeys);
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
    /// Orders the entity types so that the table of each principal is created before the tables that
    /// reference it. The server checks that the referenced table exists when it creates the child.
    /// Otherwise the model order stays.
    /// </summary>
    /// <remarks>
    /// The server refuses a cycle of two or more tables (<c>CADB0416</c>), so a model with one cannot get
    /// its foreign keys. This method refuses it before any table is created, and names the tables.
    /// </remarks>
    internal static IReadOnlyList<IEntityType> OrderForCreation(IEnumerable<IEntityType> entityTypes, bool foreignKeys)
    {
        List<IEntityType> pending = entityTypes.ToList();
        if (!foreignKeys)
            return pending;

        // The tables each table must wait for: the other tables its foreign keys reference.
        Dictionary<string, HashSet<string>> dependencies = new(StringComparer.Ordinal);
        foreach (var entityType in pending)
        {
            if (entityType.GetTableName() is not { } tableName)
                continue;

            if (!dependencies.TryGetValue(tableName, out HashSet<string>? tables))
                dependencies[tableName] = tables = new(StringComparer.Ordinal);

            foreach (var constraint in GetForeignKeyConstraints(entityType, tableName))
            {
                if (!string.Equals(constraint.PrincipalTable.Name, tableName, StringComparison.Ordinal))
                    tables.Add(constraint.PrincipalTable.Name);
            }
        }

        List<IEntityType> ordered = new(pending.Count);
        HashSet<string> created = new(StringComparer.Ordinal);

        while (pending.Count > 0)
        {
            int ready = pending.FindIndex(e =>
                e.GetTableName() is not { } t || dependencies[t].All(d => created.Contains(d) || !dependencies.ContainsKey(d)));

            if (ready < 0)
            {
                string tables = string.Join(", ", pending.Select(e => e.GetTableName()).Distinct().Select(t => $"'{t}'"));
                throw new NotSupportedException(
                    $"The foreign keys of the tables {tables} form a cycle. CamusDB refuses a foreign-key cycle " +
                    "of two or more tables (CADB0416). Remove a relationship from the cycle, or call " +
                    "UseForeignKeyConstraints(false) to create the tables without foreign keys.");
            }

            IEntityType next = pending[ready];
            pending.RemoveAt(ready);
            ordered.Add(next);

            if (next.GetTableName() is { } nextTable)
                created.Add(nextTable);
        }

        return ordered;
    }

    /// <summary>
    /// The foreign-key constraints that the foreign keys of <paramref name="entityType"/> map to in
    /// <paramref name="tableName"/>. A foreign key between two entity types that share one table (table
    /// splitting, an owned type) maps to no constraint, and so has none here.
    /// </summary>
    private static IEnumerable<IForeignKeyConstraint> GetForeignKeyConstraints(IEntityType entityType, string tableName)
    {
        HashSet<string> seen = new(StringComparer.Ordinal);

        foreach (var foreignKey in entityType.GetForeignKeys())
        {
            foreach (var constraint in foreignKey.GetMappedConstraints())
            {
                if (string.Equals(constraint.Table.Name, tableName, StringComparison.Ordinal) && seen.Add(constraint.Name))
                    yield return constraint;
            }
        }
    }

    /// <summary>
    /// Composes the <c>CREATE TABLE</c> behind <c>EnsureCreated</c>.
    ///
    /// <para>Every name is delimited through <see cref="CamusIdentifier"/>, the same rule the migration
    /// generator applies. Appending a name raw would corrupt the statement for any legitimate name that
    /// is a reserved word or carries a space, and would let a model whose identifiers are derived at
    /// runtime (tenant provisioning, a dynamic schema) compose SQL nobody wrote. The CHECK expression
    /// stays verbatim — it is a SQL fragment the model author wrote, not a name.</para>
    /// </summary>
    internal static string BuildCreateTableSql(IEntityType entityType, string tableName, bool foreignKeys = true)
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

            // HasDefaultValueSql, which includes the nextval('…') default of UseSequence and UseHiLo,
            // or HasDefaultValue converted to the provider type.
            //
            // TryGetDefaultValue is what separates a configured default from no default at all. Plain
            // GetDefaultValue answers with the CLR default of the type for a non-nullable value-type
            // column that carries no default — 0 for an INT64 column, and DateTime.MinValue for a
            // DATETIME one. That put DEFAULT ('0001-01-01T00:00:00.0000000Z') on every plain DATETIME
            // column, which the server refuses because the literal is out of range for the type.
            if (prop.GetDefaultValueSql() is { Length: > 0 } defaultSql)
                sb.Append(" DEFAULT (").Append(defaultSql).Append(')');
            else if (prop.TryGetDefaultValue(out var defaultValue) && defaultValue is not null and not DBNull)
                sb.Append(" DEFAULT (")
                  .Append(CamusMigrationsSqlGenerator.FormatDefaultValue(
                      prop.GetTypeMapping().Converter?.ConvertToProvider(defaultValue) ?? defaultValue))
                  .Append(')');

            // Only a column with an explicit strategy gets the clause, so the DDL of a model without one
            // is unchanged and still runs on a server that predates large-value storage.
            if (prop.GetStorage() is { } storage)
                sb.Append(" STORAGE ").Append(storage.ToSql());
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

        // An alternate key is a unique index on the server. A foreign key can reference only a primary
        // key or a unique index, so the principal key of HasPrincipalKey needs it.
        var storeObject = StoreObjectIdentifier.Table(tableName);
        foreach (var key in entityType.GetKeys())
        {
            if (key.IsPrimaryKey())
                continue;

            string keyName = key.GetName(storeObject)
                ?? throw new InvalidOperationException($"The alternate key on '{tableName}' has no name.");

            sb.Append(", UNIQUE KEY ").Append(CamusIdentifier.Delimit(keyName, "alternateKeyName")).Append(" (")
              .Append(string.Join(", ", key.Properties.Select(p =>
                  CamusIdentifier.Delimit(p.GetColumnName(storeObject) ?? p.GetColumnName(), "alternateKeyColumn"))))
              .Append(')');
        }

        // The server creates the index each foreign key needs on the referencing columns, because
        // EnsureCreated creates no index of the model.
        if (foreignKeys)
        {
            foreach (var constraint in GetForeignKeyConstraints(entityType, tableName))
            {
                sb.Append(", ");
                CamusForeignKeySyntax.AppendConstraint(
                    sb,
                    constraint.Name,
                    constraint.Columns.Select(c => c.Name).ToList(),
                    constraint.PrincipalTable.Name,
                    constraint.PrincipalColumns.Select(c => c.Name).ToList(),
                    constraint.OnDeleteAction,
                    ReferentialAction.NoAction);
            }
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
            "NUMERIC" or "DECIMAL"    => "NUMERIC",
            "BYTES" or "BLOB"         => BytesDdl(property),
            "DATE"                    => "DATE",
            "DATETIME" or "TIMESTAMP" => "DATETIME",
            _ => clrType == typeof(bool) ? "BOOL"
                : clrType == typeof(float) ? "FLOAT32"
                : clrType == typeof(double) ? "FLOAT64"
                : clrType == typeof(decimal) ? "NUMERIC"
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
