/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// Client-side mirror of the server's Hybrid Logical Clock (HLC) timestamp
/// (<c>Kommander.Time.HLCTimestamp</c>). An HLC timestamp is a wall-clock time plus a logical counter,
/// which the server uses to order events. The client reports it in two places:
/// <list type="bullet">
/// <item><see cref="CamusTransaction.CommitTimestamp"/>: the commit timestamp of a transaction.</item>
/// <item><see cref="CamusCacheMetadata.CachedAtHlc"/>: the time when the query result cache computed an
/// entry. The server does not send <see cref="N"/> for it, so it is zero there.</item>
/// </list>
///
/// <para>Values compare in the server's order: by <see cref="L"/>, then <see cref="C"/>, then
/// <see cref="N"/>. Two values are equal only when all three parts are equal.</para>
/// </summary>
public readonly record struct CamusHlcTimestamp : IComparable<CamusHlcTimestamp>
{
    /// <summary>The id of the node whose clock made the value. It breaks ties between two values with
    /// the same <see cref="L"/> and <see cref="C"/> from different nodes.</summary>
    [JsonPropertyName("n")]
    public int N { get; init; }

    /// <summary>The physical (wall-clock) component, in the server's time unit.</summary>
    [JsonPropertyName("l")]
    public long L { get; init; }

    /// <summary>The logical counter that breaks ties within the same physical instant.</summary>
    [JsonPropertyName("c")]
    public uint C { get; init; }

    /// <summary>
    /// Compares two values by <see cref="L"/>, then <see cref="C"/>, then <see cref="N"/>, the same
    /// order as the server's <c>HLCTimestamp</c>.
    /// </summary>
    public int CompareTo(CamusHlcTimestamp other)
    {
        if (L != other.L)
            return L < other.L ? -1 : 1;

        if (C != other.C)
            return C < other.C ? -1 : 1;

        if (N != other.N)
            return N < other.N ? -1 : 1;

        return 0;
    }

    public static bool operator <(CamusHlcTimestamp left, CamusHlcTimestamp right) => left.CompareTo(right) < 0;

    public static bool operator >(CamusHlcTimestamp left, CamusHlcTimestamp right) => left.CompareTo(right) > 0;

    public static bool operator <=(CamusHlcTimestamp left, CamusHlcTimestamp right) => left.CompareTo(right) <= 0;

    public static bool operator >=(CamusHlcTimestamp left, CamusHlcTimestamp right) => left.CompareTo(right) >= 0;

    /// <summary>True when all three parts are zero. The server uses zero for "no timestamp".</summary>
    internal bool IsZero => N == 0 && L == 0 && C == 0;

    /// <summary>
    /// The value of three wire fields, or null when all three are zero. gRPC sends C as an int64 that
    /// holds the unsigned 32-bit counter.
    /// </summary>
    internal static CamusHlcTimestamp? FromWire(int n, long l, long c)
    {
        CamusHlcTimestamp value = new() { N = n, L = l, C = unchecked((uint)c) };
        return value.IsZero ? null : value;
    }

    /// <summary>The server's format, <c>HLC(N:L:C)</c>, so a value can be found in the server logs.</summary>
    public override string ToString() => $"HLC({N}:{L}:{C})";
}
