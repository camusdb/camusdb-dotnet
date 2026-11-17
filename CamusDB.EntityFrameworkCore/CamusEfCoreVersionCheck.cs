/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Reflection;
using System.Runtime.Versioning;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Stops the provider when the process runs an EF Core major that this provider assembly was not
/// compiled against.
///
/// <para>The package has one asset per EF Core major: <c>net10.0</c> for EF Core 10 and <c>net11.0</c>
/// for EF Core 11. Both assets have the same assembly identity, so the runtime loads either one. EF Core
/// changes provider-facing members between majors (for example,
/// <c>TableValuedFunctionExpression.Arguments</c> is <c>IReadOnlyList&lt;SqlExpression&gt;</c> in EF Core
/// 10 and <c>IReadOnlyList&lt;Expression&gt;</c> in EF Core 11). With the wrong asset, the JIT fails only
/// when a query first reaches a changed member, as a <see cref="MissingMethodException"/> in the middle
/// of a translation. This check moves that failure to the first <c>UseCamusDB</c> call, with a message
/// that names both versions and both files.</para>
///
/// <para>The compiled version comes from the provider's own assembly references, so the check needs no
/// build-time data. Only the major version is compared: EF Core keeps its provider surface within a major.
/// Set the <see cref="AppContext"/> switch <see cref="DisableSwitch"/> to <see langword="true"/> to skip
/// the check.</para>
/// </summary>
internal static class CamusEfCoreVersionCheck
{
    internal const string DisableSwitch = "CamusDB.EntityFrameworkCore.DisableEfCoreVersionCheck";

    private static readonly Lazy<string?> s_error = new(FindMismatch);

    public static void EnsureCompatible()
    {
        if (AppContext.TryGetSwitch(DisableSwitch, out bool disabled) && disabled)
            return;

        if (s_error.Value is { } message)
            throw new InvalidOperationException(message);
    }

    private static string? FindMismatch()
    {
        Assembly provider = typeof(CamusEfCoreVersionCheck).Assembly;
        AssemblyName[] references = provider.GetReferencedAssemblies();

        // EF Core and EF Core Relational ship in lockstep; both are checked so that a partial upgrade is
        // reported against the assembly that moved.
        foreach (Assembly loaded in new[] { typeof(DbContext).Assembly, typeof(RelationalOptionsExtension).Assembly })
        {
            AssemblyName loadedName = loaded.GetName();
            Version? compiled = references.FirstOrDefault(r => r.Name == loadedName.Name)?.Version;

            string? message = Describe(
                loadedName.Name!,
                compiled,
                loadedName.Version,
                provider.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion,
                provider.GetCustomAttribute<TargetFrameworkAttribute>()?.FrameworkName,
                provider.Location,
                loaded.Location);

            if (message is not null)
                return message;
        }

        return null;
    }

    /// <summary>
    /// The error for one EF Core assembly, or <see langword="null"/> when the compiled and the loaded
    /// major versions agree or either version is unknown.
    /// </summary>
    internal static string? Describe(
        string efAssemblyName,
        Version? compiled,
        Version? loaded,
        string? providerVersion,
        string? providerTargetFramework,
        string? providerLocation,
        string? efLocation)
    {
        if (compiled is null || loaded is null || compiled.Major == loaded.Major)
            return null;

        // The informational version carries a "+<commit>" suffix; the package version is the part before it.
        string version = providerVersion?.Split('+')[0] ?? "(unknown version)";
        string framework = providerTargetFramework switch
        {
            null or "" => "unknown target framework",
            _ when new FrameworkName(providerTargetFramework) is { Identifier: ".NETCoreApp" } fx
                => $"net{fx.Version.Major}.{fx.Version.Minor}",
            _ => providerTargetFramework,
        };

        return $"CamusDB.EntityFrameworkCore {version} ({framework} asset, {Where(providerLocation)}) " +
               $"was compiled against {efAssemblyName} {compiled} (EF Core {compiled.Major}), " +
               $"but this process loaded {efAssemblyName} {loaded} (EF Core {loaded.Major}, {Where(efLocation)}). " +
               "EF Core changes provider members between major versions, so queries would fail later with " +
               "MissingMethodException or TypeLoadException. The package ships a net10.0 asset for EF Core 10 " +
               "and a net11.0 asset for EF Core 11. Make sure that the application targets the framework that " +
               "matches its EF Core major, and that the deployed CamusDB.EntityFrameworkCore.dll is the asset " +
               "for that framework: a clean publish folder, no copy from another build, no plugin folder with its " +
               $"own copy. To skip this check, set the AppContext switch \"{DisableSwitch}\" to true.";
    }

    private static string Where(string? location)
        => string.IsNullOrEmpty(location) ? "loaded without a file path" : $"from {location}";
}
