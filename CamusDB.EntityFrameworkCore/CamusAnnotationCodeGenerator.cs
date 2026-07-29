using System.Reflection;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace CamusDB.EntityFrameworkCore;

public class CamusAnnotationCodeGenerator : AnnotationCodeGenerator
{
    private static readonly MethodInfo IndexHasCommentMethod
        = typeof(CamusIndexBuilderExtensions).GetRuntimeMethod(
              nameof(CamusIndexBuilderExtensions.HasComment), [typeof(IndexBuilder), typeof(string)])!;

    public CamusAnnotationCodeGenerator(AnnotationCodeGeneratorDependencies dependencies)
        : base(dependencies) { }

    // Render the index comment as .HasComment("...") in scaffolded models rather than a raw
    // .HasAnnotation("Camus:IndexComment", ...) call.
    protected override MethodCallCodeFragment? GenerateFluentApi(IIndex index, IAnnotation annotation)
        => annotation.Name == CamusAnnotationNames.IndexComment && annotation.Value is string comment
            ? new MethodCallCodeFragment(IndexHasCommentMethod, comment)
            : base.GenerateFluentApi(index, annotation);
}
