namespace CamusDB.Client.Metadata;

public sealed class CamusIndexMetadata
{
    public required string Name { get; init; }

    public required IReadOnlyList<string> ColumnNames { get; init; }

    public bool IsUnique { get; init; }
}
