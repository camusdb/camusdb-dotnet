/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Stores a <c>float[]</c> property in a <c>bytes</c> column, in the <see cref="CamusVector"/> layout:
/// packed little-endian float32 elements with no header. <see cref="CamusPropertyBuilderExtensions.IsVector(Microsoft.EntityFrameworkCore.Metadata.Builders.PropertyBuilder, int, bool)"/>
/// installs it on a <c>float[]</c> property, so an entity can hold the embedding as floats.
/// </summary>
/// <remarks>
/// The conversion methods are public because a compiled model writes the converter out as C# code
/// that calls them.
/// </remarks>
public sealed class CamusVectorValueConverter : ValueConverter<float[], byte[]>
{
    /// <summary>The one instance; the converter has no state.</summary>
    public static readonly CamusVectorValueConverter Instance = new();

    public CamusVectorValueConverter()
        : base(v => ToProvider(v), v => FromProvider(v)) { }

    /// <summary>Packs a vector into the bytes the server stores.</summary>
    public static byte[] ToProvider(float[] vector) => CamusVector.ToBytes(vector);

    /// <summary>Unpacks stored bytes into a vector. A byte count that is not a multiple of four is refused.</summary>
    public static float[] FromProvider(byte[] bytes) => CamusVector.ToFloats(bytes);
}

/// <summary>
/// Compares <c>float[]</c> vectors element by element, so the change tracker sees a change made in place
/// (<c>entity.Embedding[3] = 0.5f</c>). The default comparer of an array compares references only.
/// </summary>
public sealed class CamusVectorValueComparer : ValueComparer<float[]>
{
    /// <summary>The one instance; the comparer has no state.</summary>
    public static readonly CamusVectorValueComparer Instance = new();

    public CamusVectorValueComparer()
        : base((a, b) => AreEqual(a, b), v => Hash(v), v => Copy(v)) { }

    /// <summary>True when both are null, or both have the same elements in the same order.</summary>
    public static bool AreEqual(float[]? a, float[]? b)
        => ReferenceEquals(a, b) || (a is not null && b is not null && a.AsSpan().SequenceEqual(b));

    /// <summary>A hash of the element count and every element.</summary>
    public static int Hash(float[] vector)
    {
        HashCode hash = new();
        hash.Add(vector.Length);
        foreach (float element in vector)
            hash.Add(element);

        return hash.ToHashCode();
    }

    /// <summary>A copy, so a later change in place does not also change the snapshot.</summary>
    public static float[] Copy(float[] vector) => (float[])vector.Clone();
}
