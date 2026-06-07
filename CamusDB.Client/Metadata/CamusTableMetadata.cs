namespace CamusDB.Client.Metadata;

public sealed class CamusTableMetadata
{
    public required string TableName { get; init; }

    public required IReadOnlyList<CamusColumnMetadata> Columns { get; init; }

    public CamusPrimaryKeyMetadata? PrimaryKey { get; init; }

    public required IReadOnlyList<CamusIndexMetadata> Indexes { get; init; }
}
