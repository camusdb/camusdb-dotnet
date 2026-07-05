/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json.Serialization;

namespace CamusDB.Client;

/// <summary>
/// Client-side mirror of the server's Hybrid Logical Clock timestamp
/// (<c>Kommander.Time.HLCTimestamp</c>). It is the wall-clock/logical pair the server
/// uses to order events; the query result cache reports it as the moment a served entry
/// was computed (see <see cref="CamusCacheMetadata.CachedAtHlc"/>).
/// </summary>
public readonly struct CamusHlcTimestamp
{
    /// <summary>The physical (wall-clock) component, in the server's time unit.</summary>
    [JsonPropertyName("l")]
    public long L { get; init; }

    /// <summary>The logical counter that breaks ties within the same physical instant.</summary>
    [JsonPropertyName("c")]
    public uint C { get; init; }

    public override string ToString() => $"{L}:{C}";
}
