using System.Reflection;
using CamusDB.Client;
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

    private static readonly MethodInfo PropertyHasStorageMethod
        = typeof(CamusPropertyBuilderExtensions).GetRuntimeMethod(
              nameof(CamusPropertyBuilderExtensions.HasStorage), [typeof(PropertyBuilder), typeof(CamusColumnStorage?)])!;

    private static readonly MethodInfo PropertyUseSequenceMethod
        = typeof(CamusPropertyBuilderExtensions).GetRuntimeMethod(
              nameof(CamusPropertyBuilderExtensions.UseSequence), [typeof(PropertyBuilder), typeof(string)])!;

    private static readonly MethodInfo PropertyUseHiLoMethod
        = typeof(CamusPropertyBuilderExtensions).GetRuntimeMethod(
              nameof(CamusPropertyBuilderExtensions.UseHiLo), [typeof(PropertyBuilder), typeof(string), typeof(int?)])!;

    public CamusAnnotationCodeGenerator(AnnotationCodeGeneratorDependencies dependencies)
        : base(dependencies) { }

    // Render the index comment as .HasComment("...") in scaffolded models rather than a raw
    // .HasAnnotation("Camus:IndexComment", ...) call.
    protected override MethodCallCodeFragment? GenerateFluentApi(IIndex index, IAnnotation annotation)
        => annotation.Name == CamusAnnotationNames.IndexComment && annotation.Value is string comment
            ? new MethodCallCodeFragment(IndexHasCommentMethod, comment)
            : base.GenerateFluentApi(index, annotation);

    // Render the column storage strategy as .HasStorage(CamusColumnStorage.Plain) in the model snapshot
    // and in scaffolded models.
    //
    // A sequence-backed property renders as .UseSequence("name") or .UseHiLo("name"). UseHiLo gets no
    // block size: the snapshot configures the sequence with HasSequence(...).IncrementsBy(n) already, and
    // a block size here would change that increment again.
    protected override MethodCallCodeFragment? GenerateFluentApi(IProperty property, IAnnotation annotation)
        => annotation switch
        {
            { Name: CamusAnnotationNames.ColumnStorage, Value: CamusColumnStorage storage }
                => new MethodCallCodeFragment(PropertyHasStorageMethod, storage),
            { Name: CamusAnnotationNames.SequenceName, Value: string sequenceName }
                => new MethodCallCodeFragment(PropertyUseSequenceMethod, sequenceName),
            { Name: CamusAnnotationNames.HiLoSequenceName, Value: string sequenceName }
                => new MethodCallCodeFragment(PropertyUseHiLoMethod, sequenceName, null),
            _ => base.GenerateFluentApi(property, annotation),
        };
}
