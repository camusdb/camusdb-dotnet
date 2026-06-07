namespace CamusDB.Client.Metadata;

public sealed class CamusPrimaryKeyMetadata
{
    public required string Name { get; init; }

    public required IReadOnlyList<string> ColumnNames { get; init; }
}
