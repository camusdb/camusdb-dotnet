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

    /// <summary>
    /// Sets the index access method. <c>"gin"</c> makes a GIN (inverted) index over one <c>jsonb</c>
    /// column, which serves <c>@&gt;</c>, <c>?</c>, <c>?|</c>, <c>?&amp;</c> and the equality of one key
    /// (<see cref="CamusDbFunctionsExtensions.JsonContains"/> and the other JSON functions). Migrations emit
    /// <c>CREATE INDEX … USING GIN (col)</c>, or <c>INVERTED INDEX</c> inside <c>CREATE TABLE</c>.
    /// <c>"btree"</c> or <see langword="null"/> is the ordered index, the default.
    /// </summary>
    /// <remarks>
    /// A GIN index has exactly one <c>jsonb</c> column, and it cannot be unique. The server refuses any other
    /// shape. The other methods PostgreSQL has (<c>hash</c>, <c>gist</c>, …) do not exist in CamusDB.
    /// </remarks>
    public static IndexBuilder HasMethod(this IndexBuilder indexBuilder, string? method)
    {
        indexBuilder.Metadata.SetAnnotation(CamusAnnotationNames.IndexMethod, NormalizeMethod(method));
        return indexBuilder;
    }

    /// <inheritdoc cref="HasMethod(IndexBuilder, string?)" />
    public static IndexBuilder<TEntity> HasMethod<TEntity>(this IndexBuilder<TEntity> indexBuilder, string? method)
    {
        indexBuilder.Metadata.SetAnnotation(CamusAnnotationNames.IndexMethod, NormalizeMethod(method));
        return indexBuilder;
    }

    /// <summary>Returns the access method configured on the index: <c>"gin"</c>, or null for the ordered index.</summary>
    public static string? GetMethod(this IReadOnlyIndex index)
        => (string?)index[CamusAnnotationNames.IndexMethod];

    /// <summary>True when the index is a GIN (inverted) index.</summary>
    public static bool IsGin(this IReadOnlyIndex index)
        => IsGinMethod(index[CamusAnnotationNames.IndexMethod]);

    internal static bool IsGinMethod(object? method)
        => method is string text && text.Equals(GinMethod, StringComparison.OrdinalIgnoreCase);

    private const string GinMethod = "gin";

    // "btree" is the default, so it is stored as no annotation, and a model that names it matches one that
    // does not.
    private static string? NormalizeMethod(string? method)
    {
        if (method is null || method.Equals("btree", StringComparison.OrdinalIgnoreCase))
            return null;

        if (method.Equals(GinMethod, StringComparison.OrdinalIgnoreCase))
            return GinMethod;

        throw new ArgumentException(
            $"CamusDB has no index method '{method}'. Use \"gin\" for a jsonb column, or \"btree\" (the default).",
            nameof(method));
    }

    /// <summary>Returns the CamusDB comment configured on the index, if any.</summary>
    public static string? GetComment(this IReadOnlyIndex index)
        => (string?)index[CamusAnnotationNames.IndexComment];
}
