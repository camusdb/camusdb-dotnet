namespace CamusDB.Client.Metadata;

/// <summary>
/// Provides schema metadata retrieval for a CamusDB database.
/// Metadata operations are separate from command execution and safe to call repeatedly.
/// </summary>
public sealed class CamusSchemaMetadataClient
{
    private readonly CamusConnectionStringBuilder _builder;

    public CamusSchemaMetadataClient(string connectionString)
        : this(new CamusConnectionStringBuilder(connectionString)) { }

    public CamusSchemaMetadataClient(CamusConnectionStringBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        _builder = builder;
    }

    /// <summary>
    /// Returns metadata for all tables in the configured database.
    /// </summary>
    public Task<IReadOnlyList<CamusTableMetadata>> GetTablesAsync(CancellationToken cancellationToken = default)
    {
        // TODO: implement once CamusDB exposes schema metadata endpoints
        return Task.FromResult<IReadOnlyList<CamusTableMetadata>>(Array.Empty<CamusTableMetadata>());
    }

    /// <summary>
    /// Returns metadata for a specific table, or null if the table does not exist.
    /// </summary>
    public Task<CamusTableMetadata?> GetTableAsync(string tableName, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(tableName);

        // TODO: implement once CamusDB exposes schema metadata endpoints
        return Task.FromResult<CamusTableMetadata?>(null);
    }
}
