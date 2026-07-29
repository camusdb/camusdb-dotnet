using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Carries CamusDB-specific annotations from the EF model onto the relational model, from where the
/// migrations differ copies them onto the generated operations.
/// </summary>
public class CamusRelationalAnnotationProvider : RelationalAnnotationProvider
{
    public CamusRelationalAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
        : base(dependencies) { }

    public override IEnumerable<IAnnotation> For(ITableIndex index, bool designTime)
    {
        foreach (var annotation in base.For(index, designTime))
            yield return annotation;

        if (!designTime)
            yield break;

        // A table index can be backed by several model indexes (TPH hierarchies); the first one
        // carrying a comment wins, matching how EF resolves the other index facets.
        foreach (var modelIndex in index.MappedIndexes)
        {
            if (modelIndex.FindAnnotation(CamusAnnotationNames.IndexComment) is { Value: string comment })
            {
                yield return new Annotation(CamusAnnotationNames.IndexComment, comment);
                yield break;
            }
        }
    }
}
