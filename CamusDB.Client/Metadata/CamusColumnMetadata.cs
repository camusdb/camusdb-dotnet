namespace CamusDB.Client.Metadata;

public sealed class CamusColumnMetadata
{
    public required string Name { get; init; }

    public required string ProviderTypeName { get; init; }

    public ColumnType ColumnType { get; init; }

    public bool IsNullable { get; init; }

    public bool IsPrimaryKey { get; init; }
}
