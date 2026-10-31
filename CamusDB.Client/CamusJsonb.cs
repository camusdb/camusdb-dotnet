/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace CamusDB.Client;

/// <summary>
/// Conversions between a CamusDB <see cref="ColumnType.Jsonb"/> value and .NET types.
///
/// <para>A <c>jsonb</c> cell holds one JSON document. It travels as JSON text in
/// <see cref="ColumnValue.StrValue"/>. The reader gives the PostgreSQL <c>jsonb_out</c> text form on
/// every transport: a space after each <c>:</c> and each <c>,</c>, keys in the stored order and no other
/// white space, for example <c>{"a": [1, 2.50], "b": null}</c>. Numbers keep every digit and their
/// scale. Parse the text with a JSON reader that keeps numbers as text or decimals when exact values
/// matter.</para>
///
/// <para>JSON <c>null</c> and SQL <c>NULL</c> are different values. Over gRPC a document that is JSON
/// <c>null</c> reads as the text <c>null</c>. A REST row writes both as the token <c>null</c>, so over REST
/// a top-level JSON <c>null</c> document reads as SQL <c>NULL</c>.</para>
/// </summary>
public static class CamusJsonb
{
    /// <summary>The deepest document the server stores (<c>JsonNestingTooDeep</c>, <c>CADB0419</c>).</summary>
    public const int MaxNestingDepth = 512;

    /// <summary>
    /// The reader depth of a response that can carry documents: the deepest document, and the levels
    /// of the response around it (the root object, the row list, a row and an array cell).
    /// </summary>
    internal const int ResponseMaxDepth = MaxNestingDepth + 16;

    internal static readonly JsonDocumentOptions DocumentOptions = new() { MaxDepth = ResponseMaxDepth };

    internal static readonly JsonReaderOptions ReaderOptions = new() { MaxDepth = ResponseMaxDepth };

    private static readonly JsonNodeOptions NodeOptions = new();

    /// <summary>
    /// True when <paramref name="value"/> is a value that binds as <c>jsonb</c> with no explicit column
    /// type: a <see cref="JsonDocument"/>, a <see cref="JsonElement"/> or a <see cref="JsonNode"/>.
    /// A <see cref="string"/> is not one: it binds as a STRING, which a <c>jsonb</c> column parses when
    /// it is written.
    /// </summary>
    internal static bool IsDocumentValue(object value) => value is JsonDocument or JsonElement or JsonNode;

    /// <summary>True when <paramref name="type"/> is a CLR type that binds as <c>jsonb</c>.</summary>
    internal static bool IsDocumentType(Type type)
        => type == typeof(JsonDocument) || type == typeof(JsonElement) || typeof(JsonNode).IsAssignableFrom(type);

    /// <summary>
    /// The JSON text that binds a parameter value as <c>jsonb</c>. A <see cref="string"/> passes through
    /// as it is; the server validates it and refuses text that is not JSON with <c>CADB0418</c>.
    /// </summary>
    internal static string FromParameter(string name, object value) => value switch
    {
        string s => s,
        JsonDocument document => document.RootElement.GetRawText(),
        JsonElement { ValueKind: JsonValueKind.Undefined } =>
            throw new CamusException("CADB0400", $"JSONB parameter '{name}' is a default JsonElement, which holds no value"),
        JsonElement element => element.GetRawText(),
        JsonNode node => node.ToJsonString(),
        ReadOnlyMemory<byte> utf8 => Encoding.UTF8.GetString(utf8.Span),
        byte[] utf8 => Encoding.UTF8.GetString(utf8),
        _ => throw new CamusException("CADB0400", $"Cannot map JSONB parameter '{name}' from {value.GetType().Name}")
    };

    /// <summary>
    /// The PostgreSQL text form of <paramref name="json"/>: one space after each <c>:</c> and each
    /// <c>,</c>, and no other white space. Keys keep their order, and numbers keep their text. Throws
    /// <see cref="JsonException"/> when <paramref name="json"/> is not one JSON value.
    /// </summary>
    public static string Format(string json)
    {
        byte[] utf8 = Encoding.UTF8.GetBytes(json);
        Utf8JsonReader reader = new(utf8, ReaderOptions);

        if (!reader.Read())
            throw new JsonException("The text holds no JSON value.");

        string text = ToText(ref reader);

        if (reader.Read())
            throw new JsonException("The text holds more than one JSON value.");

        return text;
    }

    /// <summary>
    /// Reads the value at <paramref name="reader"/>'s current token in the PostgreSQL text form. The
    /// reader is on the value's first token, and it is on the value's last token on return.
    /// </summary>
    internal static string ToText(ref Utf8JsonReader reader)
    {
        // A scalar document is short; a container is not, and it reads into one builder.
        switch (reader.TokenType)
        {
            case JsonTokenType.Number:
                return Encoding.ASCII.GetString(reader.ValueSpan);

            case JsonTokenType.True:
                return "true";

            case JsonTokenType.False:
                return "false";

            case JsonTokenType.Null:
                return "null";
        }

        StringBuilder text = new();
        AppendText(ref reader, text);
        return text.ToString();
    }

    /// <summary>The <see cref="JsonElement"/> form of <see cref="ToText(ref Utf8JsonReader)"/>.</summary>
    internal static string ToText(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Number:
                return element.GetRawText();

            case JsonValueKind.True:
                return "true";

            case JsonValueKind.False:
                return "false";

            case JsonValueKind.Null:
                return "null";
        }

        byte[] utf8 = Encoding.UTF8.GetBytes(element.GetRawText());
        Utf8JsonReader reader = new(utf8, ReaderOptions);
        reader.Read();
        return ToText(ref reader);
    }

    // The walk keeps one flag: whether the next value or key needs a ", " before it. The value ends on
    // the token at the depth it started at that does not open a container.
    private static void AppendText(ref Utf8JsonReader reader, StringBuilder text)
    {
        int depth = reader.CurrentDepth;
        bool needSeparator = false;

        while (true)
        {
            JsonTokenType token = reader.TokenType;

            switch (token)
            {
                case JsonTokenType.EndObject:
                    text.Append('}');
                    needSeparator = true;
                    break;

                case JsonTokenType.EndArray:
                    text.Append(']');
                    needSeparator = true;
                    break;

                case JsonTokenType.PropertyName:
                    if (needSeparator)
                        text.Append(", ");
                    AppendString(text, reader.GetString()!);
                    text.Append(": ");
                    needSeparator = false;
                    break;

                default:
                    if (needSeparator)
                        text.Append(", ");
                    AppendValue(ref reader, token, text);
                    needSeparator = token is not (JsonTokenType.StartObject or JsonTokenType.StartArray);
                    break;
            }

            if (reader.CurrentDepth == depth && token is not (JsonTokenType.StartObject or JsonTokenType.StartArray))
                return;

            if (!reader.Read())
                throw new JsonException("The JSON value ended inside a container.");
        }
    }

    private static void AppendValue(ref Utf8JsonReader reader, JsonTokenType token, StringBuilder text)
    {
        switch (token)
        {
            case JsonTokenType.StartObject:
                text.Append('{');
                break;

            case JsonTokenType.StartArray:
                text.Append('[');
                break;

            case JsonTokenType.String:
                AppendString(text, reader.GetString()!);
                break;

            case JsonTokenType.Number:
                // A number token is ASCII. Its text is kept, so every digit and the scale survive.
                text.Append(Encoding.ASCII.GetString(reader.ValueSpan));
                break;

            case JsonTokenType.True:
                text.Append("true");
                break;

            case JsonTokenType.False:
                text.Append("false");
                break;

            default:
                text.Append("null");
                break;
        }
    }

    // The PostgreSQL escape_json rules: a quote, a backslash and each control character are escaped,
    // and every other character is written as it is.
    private static void AppendString(StringBuilder text, string value)
    {
        text.Append('"');

        foreach (char ch in value)
        {
            switch (ch)
            {
                case '"': text.Append("\\\""); break;
                case '\\': text.Append("\\\\"); break;
                case '\b': text.Append("\\b"); break;
                case '\f': text.Append("\\f"); break;
                case '\n': text.Append("\\n"); break;
                case '\r': text.Append("\\r"); break;
                case '\t': text.Append("\\t"); break;
                default:
                    if (ch < ' ')
                        text.Append("\\u").Append(((int)ch).ToString("x4", System.Globalization.CultureInfo.InvariantCulture));
                    else
                        text.Append(ch);
                    break;
            }
        }

        text.Append('"');
    }

    /// <summary>
    /// Parses document text as <paramref name="target"/>: <see cref="JsonDocument"/>,
    /// <see cref="JsonElement"/>, <see cref="JsonNode"/>, <see cref="JsonObject"/>, <see cref="JsonArray"/>
    /// or <see cref="JsonValue"/>; the caller checks <see cref="IsDocumentType"/> first. A document of the
    /// wrong kind for a <see cref="JsonObject"/>, <see cref="JsonArray"/> or <see cref="JsonValue"/> target
    /// throws <see cref="InvalidCastException"/>.
    /// </summary>
    internal static object? Parse(string text, Type target)
    {
        if (target == typeof(JsonDocument))
            return JsonDocument.Parse(text, DocumentOptions);

        if (target == typeof(JsonElement))
        {
            using JsonDocument document = JsonDocument.Parse(text, DocumentOptions);
            return document.RootElement.Clone();
        }

        if (!typeof(JsonNode).IsAssignableFrom(target))
            throw new ArgumentException($"{target.Name} is not a JSON document type.", nameof(target));

        JsonNode? node = JsonNode.Parse(text, NodeOptions, DocumentOptions);

        // JsonNode.Parse gives null for the document null. A JsonNode target takes that null; a narrower
        // target cannot hold it.
        if (node is null)
            return target == typeof(JsonNode)
                ? null
                : throw new InvalidCastException($"The JSONB document null cannot be read as {target.Name}.");

        if (!target.IsInstanceOfType(node))
            throw new InvalidCastException($"The JSONB document is a {node.GetValueKind()}, which cannot be read as {target.Name}.");

        return node;
    }
}
