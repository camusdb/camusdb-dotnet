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
}
