namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Names of the CamusDB-specific annotations stored on the EF Core model.
/// </summary>
public static class CamusAnnotationNames
{
    public const string Prefix = "Camus:";

    /// <summary>
    /// Free-text description attached to an index, applied with COMMENT ON INDEX. EF Core has no
    /// built-in comment surface for indexes, unlike tables and columns.
    /// </summary>
    public const string IndexComment = Prefix + "IndexComment";

    /// <summary>
    /// The <see cref="CamusDB.Client.CamusColumnStorage"/> of a <c>string</c>, <c>bytes</c> or array
    /// column, emitted as an inline <c>STORAGE</c> clause and changed with <c>ALTER COLUMN … SET STORAGE</c>.
    /// Absent means the server default, <c>EXTENDED</c>.
    /// </summary>
    public const string ColumnStorage = Prefix + "ColumnStorage";
}
