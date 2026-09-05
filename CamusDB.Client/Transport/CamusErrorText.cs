/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDB.Client.Transport;

/// <summary>
/// Cleans the text that becomes a <see cref="CamusException"/> message.
///
/// <para><b>Why.</b> The message is built from whatever the far end returned — a server error body, a
/// gRPC status detail, an HTTP client's own exception text. Applications log it almost without
/// exception, so three properties of that text matter and none of them are guaranteed by the sender:
/// it must not carry a credential, it must not carry line breaks that let it forge log entries, and it
/// must not be unbounded.</para>
///
/// <para>Only the error path pays for this, so the scan is a plain single pass rather than a set of
/// regular expressions.</para>
/// </summary>
internal static class CamusErrorText
{
    /// <summary>Longest message kept. A server error body is a sentence; anything longer is a page of
    /// content that has been echoed back, and it reaches a log line either way.</summary>
    public const int MaxLength = 2048;

    private const string Elided = "…";

    private const string Masked = "***";

    /// <summary>Credential-shaped prefixes whose following token is masked.</summary>
    private static readonly string[] SecretPrefixes = ["bearer ", "password=", "password\":", "pwd=", "accesstoken=", "token\":"];

    /// <summary>
    /// Returns <paramref name="text"/> with control characters folded to spaces, credential-shaped runs
    /// masked, and the length bounded.
    /// </summary>
    public static string Sanitize(string? text)
    {
        if (string.IsNullOrEmpty(text))
            return "";

        StringBuilder clean = new(Math.Min(text.Length, MaxLength) + Masked.Length);

        int index = 0;

        while (index < text.Length && clean.Length < MaxLength)
        {
            if (MatchSecretPrefix(text, index) is { } prefixLength)
            {
                clean.Append(text, index, prefixLength).Append(Masked);
                index = SkipSecretValue(text, index + prefixLength);
                continue;
            }

            char c = text[index++];

            // A newline in a logged message lets the sender forge a second log entry; other control
            // characters can rewrite a terminal. Both become a plain space.
            clean.Append(char.IsControl(c) ? ' ' : c);
        }

        if (index < text.Length)
            clean.Append(Elided);

        return clean.ToString();
    }

    /// <summary>The length of the credential-shaped prefix starting at <paramref name="index"/>, or null.</summary>
    private static int? MatchSecretPrefix(string text, int index)
    {
        foreach (string prefix in SecretPrefixes)
        {
            if (index + prefix.Length <= text.Length &&
                text.AsSpan(index, prefix.Length).Equals(prefix, StringComparison.OrdinalIgnoreCase))
                return prefix.Length;
        }

        return null;
    }

    /// <summary>
    /// Skips the value a credential-shaped prefix introduces: leading quotes and spaces, then everything
    /// up to the first character that cannot be part of a token or a quoted value.
    /// </summary>
    private static int SkipSecretValue(string text, int index)
    {
        while (index < text.Length && (text[index] is ' ' or '"' or '\''))
            index++;

        while (index < text.Length && text[index] is not (' ' or '"' or '\'' or ',' or ';' or '}' or ')' or '\n' or '\r'))
            index++;

        return index;
    }
}
