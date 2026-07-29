using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// CamusDB-specific index configuration.
/// </summary>
public static class CamusIndexBuilderExtensions
{
    /// <summary>
    /// Attaches a free-text description to the index, emitted as COMMENT ON INDEX by migrations.
    /// Pass <see langword="null"/> to remove an existing comment.
    /// </summary>
    /// <remarks>
    /// The primary key index cannot carry a comment in CamusDB — SHOW CREATE TABLE renders it as a
    /// bare PRIMARY KEY line with no inline COMMENT form to round-trip through. Comment the table
    /// instead.
    /// </remarks>
    public static IndexBuilder HasComment(this IndexBuilder indexBuilder, string? comment)
    {
        indexBuilder.Metadata.SetAnnotation(CamusAnnotationNames.IndexComment, comment);
        return indexBuilder;
    }

    /// <inheritdoc cref="HasComment(IndexBuilder, string?)" />
    public static IndexBuilder<TEntity> HasComment<TEntity>(this IndexBuilder<TEntity> indexBuilder, string? comment)
    {
        indexBuilder.Metadata.SetAnnotation(CamusAnnotationNames.IndexComment, comment);
        return indexBuilder;
    }

    /// <summary>Returns the CamusDB comment configured on the index, if any.</summary>
    public static string? GetComment(this IReadOnlyIndex index)
        => (string?)index[CamusAnnotationNames.IndexComment];
}
