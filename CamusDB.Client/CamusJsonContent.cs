using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace CamusDB.Client;

internal static class CamusJsonContent
{
    private static readonly MediaTypeHeaderValue JsonMediaType = new("application/json") { CharSet = "utf-8" };

    /// <summary>
    /// Serializes <paramref name="value"/> straight to UTF-8 bytes and wraps them in a
    /// <see cref="ByteArrayContent"/>. This avoids the intermediate UTF-16 <see cref="string"/> that
    /// <c>JsonSerializer.Serialize</c> + <see cref="StringContent"/> would allocate, and the subsequent
    /// UTF-16 → UTF-8 re-encode <see cref="StringContent"/> performs when the request body is written.
    /// </summary>
    public static ByteArrayContent Create<T>(T value, JsonTypeInfo<T> typeInfo)
    {
        byte[] utf8 = JsonSerializer.SerializeToUtf8Bytes(value, typeInfo);
        ByteArrayContent content = new(utf8);
        content.Headers.ContentType = JsonMediaType;
        return content;
    }
}
