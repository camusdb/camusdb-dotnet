/**
 * This file is part of CamusDB
 *
 * The EF Core provider has one asset per EF Core major (net10.0 for EF Core 10, net11.0 for EF Core 11).
 * EF Core changes provider-facing members between majors, so the wrong asset fails with
 * MissingMethodException only when a query first reaches a changed member. The 0.19.0 report (Vorpal
 * 4c9f8a28) was the net10.0 asset in a process that loaded EF Core 11:
 * TableValuedFunctionExpression.Arguments is IReadOnlyList<SqlExpression> in EF Core 10 and
 * IReadOnlyList<Expression> in EF Core 11, so the first Contains query failed in TryGetArray.
 *
 * These tests run on each target framework and need no server.
 */

using System.Collections.Immutable;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkCoreVersionCheck
{
    [Fact]
    public void ProviderBuildMatchesTheLoadedEfCore()
    {
        CamusEfCoreVersionCheck.EnsureCompatible();

        // UseCamusDB runs the same check.
        new DbContextOptionsBuilder().UseCamusDB("Endpoint=http://localhost:5095;Database=test");
    }

    [Fact]
    public void Net10AssetOnEfCore11IsRefused()
    {
        string? message = CamusEfCoreVersionCheck.Describe(
            "Microsoft.EntityFrameworkCore.Relational",
            compiled: new Version(10, 0, 13, 0),
            loaded: new Version(11, 0, 0, 0),
            providerVersion: "0.19.0+0bd5198",
            providerTargetFramework: ".NETCoreApp,Version=v10.0",
            providerLocation: "/app/CamusDB.EntityFrameworkCore.dll",
            efLocation: "/app/Microsoft.EntityFrameworkCore.Relational.dll");

        Assert.NotNull(message);
        Assert.Contains("CamusDB.EntityFrameworkCore 0.19.0 (net10.0 asset, from /app/CamusDB.EntityFrameworkCore.dll)", message);
        Assert.Contains("compiled against Microsoft.EntityFrameworkCore.Relational 10.0.13.0 (EF Core 10)", message);
        Assert.Contains("loaded Microsoft.EntityFrameworkCore.Relational 11.0.0.0 (EF Core 11", message);
        Assert.Contains(CamusEfCoreVersionCheck.DisableSwitch, message);
    }

    [Fact]
    public void Net11AssetOnEfCore10IsRefused()
    {
        string? message = CamusEfCoreVersionCheck.Describe(
            "Microsoft.EntityFrameworkCore",
            compiled: new Version(11, 0, 0, 0),
            loaded: new Version(10, 0, 13, 0),
            providerVersion: null,
            providerTargetFramework: null,
            providerLocation: "",
            efLocation: null);

        Assert.NotNull(message);
        Assert.Contains("(unknown version)", message);
        Assert.Contains("loaded without a file path", message);
    }

    [Theory]
    [InlineData(10, 10, 0, 13)]
    [InlineData(11, 11, 0, 0)]
    [InlineData(11, 11, 2, 5)]
    public void SameMajorIsAccepted(int compiledMajor, int loadedMajor, int loadedMinor, int loadedBuild)
    {
        Assert.Null(CamusEfCoreVersionCheck.Describe(
            "Microsoft.EntityFrameworkCore.Relational",
            new Version(compiledMajor, 0, 0, 0),
            new Version(loadedMajor, loadedMinor, loadedBuild, 0),
            "0.19.1", ".NETCoreApp,Version=v11.0", "/p.dll", "/r.dll"));
    }

    [Fact]
    public void UnknownVersionIsAccepted()
    {
        Assert.Null(CamusEfCoreVersionCheck.Describe("Microsoft.EntityFrameworkCore", null, new Version(11, 0), null, null, null, null));
        Assert.Null(CamusEfCoreVersionCheck.Describe("Microsoft.EntityFrameworkCore", new Version(11, 0), null, null, null, null, null));
    }

    /// <summary>
    /// Every member that the provider assembly references in an EF Core assembly exists, with the same
    /// signature, in the EF Core assemblies of this process. A provider build against another EF Core
    /// build (a preview, or the other major) fails here and not in a query in production.
    /// </summary>
    [Fact]
    public void EveryEfCoreMemberReferenceResolves()
    {
        Assembly provider = typeof(CamusDBOptionsExtension).Assembly;
        Assembly[] efAssemblies = [typeof(DbContext).Assembly, typeof(RelationalOptionsExtension).Assembly];

        HashSet<string> definitions = new(StringComparer.Ordinal);
        foreach (Assembly ef in efAssemblies)
            AddDefinitions(ef.Location, definitions);

        HashSet<string> efNames = efAssemblies.Select(a => a.GetName().Name!).ToHashSet(StringComparer.Ordinal);
        (int checkedCount, List<string> unresolved) = ResolveReferences(provider.Location, efNames, definitions);

        Assert.True(checkedCount > 100, $"Only {checkedCount} EF Core member references found; the scan reads the wrong assembly.");
        Assert.True(unresolved.Count == 0,
            $"{unresolved.Count} of {checkedCount} EF Core member references do not resolve against " +
            $"{string.Join(", ", efAssemblies.Select(a => a.GetName().FullName))}:\n" + string.Join("\n", unresolved));
    }

    private static void AddDefinitions(string path, HashSet<string> definitions)
    {
        using FileStream stream = File.OpenRead(path);
        using PEReader pe = new(stream);
        MetadataReader md = pe.GetMetadataReader();
        SignatureNames names = new();

        foreach (TypeDefinitionHandle typeHandle in md.TypeDefinitions)
        {
            TypeDefinition type = md.GetTypeDefinition(typeHandle);
            string typeName = SignatureNames.DefinitionName(md, typeHandle);

            foreach (MethodDefinitionHandle handle in type.GetMethods())
            {
                MethodDefinition method = md.GetMethodDefinition(handle);
                definitions.Add(MethodKey(typeName, md.GetString(method.Name), method.DecodeSignature(names, null)));
            }

            foreach (FieldDefinitionHandle handle in type.GetFields())
            {
                FieldDefinition field = md.GetFieldDefinition(handle);
                definitions.Add($"{typeName}::{md.GetString(field.Name)} : {field.DecodeSignature(names, null)}");
            }
        }
    }

    private static (int Checked, List<string> Unresolved) ResolveReferences(string path, HashSet<string> efAssemblyNames, HashSet<string> definitions)
    {
        using FileStream stream = File.OpenRead(path);
        using PEReader pe = new(stream);
        MetadataReader md = pe.GetMetadataReader();
        SignatureNames names = new();

        int checkedCount = 0;
        List<string> unresolved = [];

        foreach (MemberReferenceHandle handle in md.MemberReferences)
        {
            MemberReference member = md.GetMemberReference(handle);

            TypeReferenceHandle? parentType = member.Parent.Kind switch
            {
                HandleKind.TypeReference => (TypeReferenceHandle)member.Parent,
                HandleKind.TypeSpecification => GenericTypeOf(md, (TypeSpecificationHandle)member.Parent),
                _ => null,
            };

            if (parentType is not { } typeHandle || !efAssemblyNames.Contains(SignatureNames.AssemblyOf(md, typeHandle)))
                continue;

            string typeName = SignatureNames.ReferenceName(md, typeHandle);
            string name = md.GetString(member.Name);
            string key = member.GetKind() == MemberReferenceKind.Method
                ? MethodKey(typeName, name, member.DecodeMethodSignature(names, null))
                : $"{typeName}::{name} : {member.DecodeFieldSignature(names, null)}";

            checkedCount++;
            if (!definitions.Contains(key))
                unresolved.Add(key);
        }

        return (checkedCount, unresolved);
    }

    // A member of a generic instantiation (List<T> style) is referenced through a TypeSpec; its definition
    // lives on the generic type, whose signatures use the type parameters, as the reference's do.
    private static TypeReferenceHandle? GenericTypeOf(MetadataReader md, TypeSpecificationHandle handle)
    {
        BlobReader blob = md.GetBlobReader(md.GetTypeSpecification(handle).Signature);
        if (blob.ReadSignatureTypeCode() != SignatureTypeCode.GenericTypeInstance)
            return null;

        blob.ReadSignatureTypeCode(); // CLASS or VALUETYPE
        EntityHandle generic = blob.ReadTypeHandle();
        return generic.Kind == HandleKind.TypeReference ? (TypeReferenceHandle)generic : null;
    }

    private static string MethodKey(string typeName, string name, MethodSignature<string> signature)
        => $"{typeName}::{name}`{signature.GenericParameterCount} : {signature.ReturnType} ({string.Join(", ", signature.ParameterTypes)})";

    /// <summary>Spells a signature type by namespace and name, the same way for a definition and a reference.</summary>
    private sealed class SignatureNames : ISignatureTypeProvider<string, object?>
    {
        public static string DefinitionName(MetadataReader md, TypeDefinitionHandle handle)
        {
            TypeDefinition type = md.GetTypeDefinition(handle);
            TypeDefinitionHandle declaring = type.GetDeclaringType();
            return declaring.IsNil
                ? $"{md.GetString(type.Namespace)}.{md.GetString(type.Name)}"
                : $"{DefinitionName(md, declaring)}/{md.GetString(type.Name)}";
        }

        public static string ReferenceName(MetadataReader md, TypeReferenceHandle handle)
        {
            TypeReference type = md.GetTypeReference(handle);
            return type.ResolutionScope.Kind == HandleKind.TypeReference
                ? $"{ReferenceName(md, (TypeReferenceHandle)type.ResolutionScope)}/{md.GetString(type.Name)}"
                : $"{md.GetString(type.Namespace)}.{md.GetString(type.Name)}";
        }

        public static string AssemblyOf(MetadataReader md, TypeReferenceHandle handle)
        {
            TypeReference type = md.GetTypeReference(handle);
            return type.ResolutionScope.Kind switch
            {
                HandleKind.TypeReference => AssemblyOf(md, (TypeReferenceHandle)type.ResolutionScope),
                HandleKind.AssemblyReference => md.GetString(md.GetAssemblyReference((AssemblyReferenceHandle)type.ResolutionScope).Name),
                _ => "",
            };
        }

        public string GetArrayType(string elementType, ArrayShape shape) => $"{elementType}[{new string(',', shape.Rank - 1)}]";
        public string GetByReferenceType(string elementType) => elementType + "&";
        public string GetFunctionPointerType(MethodSignature<string> signature) => $"fnptr({signature.ReturnType}; {string.Join(", ", signature.ParameterTypes)})";
        public string GetGenericInstantiation(string genericType, ImmutableArray<string> typeArguments) => $"{genericType}<{string.Join(",", typeArguments)}>";
        public string GetGenericMethodParameter(object? genericContext, int index) => "!!" + index;
        public string GetGenericTypeParameter(object? genericContext, int index) => "!" + index;
        public string GetModifiedType(string modifier, string unmodifiedType, bool isRequired) => isRequired ? $"{unmodifiedType} modreq({modifier})" : unmodifiedType;
        public string GetPinnedType(string elementType) => elementType;
        public string GetPointerType(string elementType) => elementType + "*";
        public string GetPrimitiveType(PrimitiveTypeCode typeCode) => typeCode.ToString();
        public string GetSZArrayType(string elementType) => elementType + "[]";
        public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind) => DefinitionName(reader, handle);
        public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind) => ReferenceName(reader, handle);
        public string GetTypeFromSpecification(MetadataReader reader, object? genericContext, TypeSpecificationHandle handle, byte rawTypeKind)
            => reader.GetTypeSpecification(handle).DecodeSignature(this, genericContext);
    }
}
