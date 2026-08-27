/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace CamusDB.Client;

/// <summary>
/// Converts a float32 embedding to and from the byte layout CamusDB reads it in.
///
/// <para>CamusDB has no native vector type. A vector is a <c>bytes</c> column holding tightly packed
/// little-endian IEEE-754 float32 elements with no header, so its dimension is the byte count divided
/// by four: a 768-element embedding occupies 3 072 bytes. That layout is the whole contract, and
/// nothing in the schema records it — a value one client writes is readable by another only because
/// both follow it. This class is that layout in one place, so a caller does not restate it.</para>
///
/// <para>The declared column width is a maximum, not a width: <c>bytes(3072)</c> accepts a shorter
/// value. A <c>CHECK (vector_dims(embedding) = 768)</c> is what pins the dimension, and only
/// <c>NOT NULL</c> forbids a missing vector.</para>
/// </summary>
public static class CamusVector
{
    /// <summary>
    /// Packs <paramref name="vector"/> into a new byte array in the layout the server reads.
    /// </summary>
    public static byte[] ToBytes(ReadOnlySpan<float> vector)
    {
        byte[] bytes = new byte[vector.Length * sizeof(float)];

        WriteTo(vector, bytes);

        return bytes;
    }

    /// <summary>
    /// Packs <paramref name="vector"/> into <paramref name="destination"/>, which must hold at least
    /// four bytes per element. Use this to fill a buffer the caller already owns.
    /// </summary>
    public static void WriteTo(ReadOnlySpan<float> vector, Span<byte> destination)
    {
        int required = vector.Length * sizeof(float);

        if (destination.Length < required)
            throw new ArgumentException(
                $"A {vector.Length}-element vector needs {required} bytes; the destination holds {destination.Length}.",
                nameof(destination));

        // On a little-endian host the in-memory layout is already the wire layout, so the whole vector
        // is one copy. Every other host writes element by element, because the layout is defined as
        // little-endian rather than as whatever the host happens to use.
        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.AsBytes(vector).CopyTo(destination);
            return;
        }

        for (int i = 0; i < vector.Length; i++)
            BinaryPrimitives.WriteSingleLittleEndian(destination[(i * sizeof(float))..], vector[i]);
    }

    /// <summary>
    /// Unpacks a stored vector into a new float array.
    /// </summary>
    /// <exception cref="CamusException">
    /// <c>CADB0410</c> when the byte count is not a multiple of four, the same code and the same reason
    /// the server uses: a byte count that cannot be a vector is refused rather than rounded down.
    /// </exception>
    public static float[] ToFloats(ReadOnlySpan<byte> bytes)
    {
        float[] vector = new float[Dimensions(bytes)];

        ReadTo(bytes, vector);

        return vector;
    }

    /// <summary>
    /// Unpacks a stored vector into <paramref name="destination"/>, which must hold one element per
    /// four bytes.
    /// </summary>
    public static void ReadTo(ReadOnlySpan<byte> bytes, Span<float> destination)
    {
        int dimensions = Dimensions(bytes);

        if (destination.Length < dimensions)
            throw new ArgumentException(
                $"A {bytes.Length}-byte vector has {dimensions} elements; the destination holds {destination.Length}.",
                nameof(destination));

        if (BitConverter.IsLittleEndian)
        {
            MemoryMarshal.Cast<byte, float>(bytes).CopyTo(destination);
            return;
        }

        for (int i = 0; i < dimensions; i++)
            destination[i] = BinaryPrimitives.ReadSingleLittleEndian(bytes[(i * sizeof(float))..]);
    }

    /// <summary>
    /// The element count of a stored vector: its byte count divided by four. This is what the server's
    /// <c>vector_dims</c> reports.
    /// </summary>
    /// <exception cref="CamusException">
    /// <c>CADB0410</c> when the byte count is not a multiple of four.
    /// </exception>
    public static int Dimensions(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length % sizeof(float) != 0)
            throw new CamusException(
                "CADB0410",
                $"A vector's byte count must be a multiple of 4; got {bytes.Length}.");

        return bytes.Length / sizeof(float);
    }
}
