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

    /// <summary>
    /// The sequence that gives a property its value, one <c>nextval</c> for each value. Set by
    /// <see cref="CamusPropertyBuilderExtensions.UseSequence(Microsoft.EntityFrameworkCore.Metadata.Builders.PropertyBuilder, string)"/>.
    /// </summary>
    public const string SequenceName = Prefix + "SequenceName";

    /// <summary>
    /// The sequence that gives a property its values in blocks, one <c>nextval</c> for each block. Set by
    /// <see cref="CamusPropertyBuilderExtensions.UseHiLo(Microsoft.EntityFrameworkCore.Metadata.Builders.PropertyBuilder, string, int?)"/>.
    /// </summary>
    public const string HiLoSequenceName = Prefix + "HiLoSequenceName";
}
