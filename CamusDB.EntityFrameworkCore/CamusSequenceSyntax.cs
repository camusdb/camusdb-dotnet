/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Globalization;
using System.Text;
using CamusDB.Client;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// The sequence statements the provider composes as text: <c>CREATE SEQUENCE</c> for migrations and
/// <c>EnsureCreated</c>, and the <c>nextval('…')</c> call behind a sequence-backed column.
/// </summary>
/// <remarks>
/// <para>A CamusDB sequence stores <c>int64</c> values only, has a positive increment, and never
/// cycles. The server refuses <c>CYCLE</c> with <c>CADB0533</c>, because a wrapped counter would
/// reissue values that committed rows already hold. The provider refuses a cyclic EF sequence before
/// the round trip, with a message that names the sequence.</para>
///
/// <para>A <see langword="null"/> minimum means the server default, 1, in every statement. The server
/// reads <c>NO MINVALUE</c> as the smallest 64-bit value, not as the default, so the provider never
/// emits it.</para>
/// </remarks>
internal static class CamusSequenceSyntax
{
    /// <summary>The minimum a sequence gets when its definition names none.</summary>
    internal const long DefaultMinValue = 1;

    /// <summary>
    /// The CLR types a sequence can declare. Every one of them fits in the <c>int64</c> the server stores.
    /// </summary>
    internal static bool IsSupportedType(Type type)
        => type == typeof(long) || type == typeof(int) || type == typeof(short) || type == typeof(byte);

    /// <summary>Composes <c>CREATE SEQUENCE [IF NOT EXISTS] name START WITH … INCREMENT BY … [MINVALUE …] [MAXVALUE …]</c>.</summary>
    internal static string Create(
        string name,
        Type clrType,
        long startValue,
        int incrementBy,
        long? minValue,
        long? maxValue,
        bool isCyclic,
        bool ifNotExists)
    {
        EnsureSupported(name, clrType, isCyclic);

        StringBuilder sql = new();
        sql.Append(ifNotExists ? "CREATE SEQUENCE IF NOT EXISTS " : "CREATE SEQUENCE ")
           .Append(CamusIdentifier.Delimit(name, nameof(name)))
           .Append(" START WITH ").Append(Number(startValue))
           .Append(" INCREMENT BY ").Append(Number(incrementBy));

        if (minValue is { } min)
            sql.Append(" MINVALUE ").Append(Number(min));

        if (maxValue is { } max)
            sql.Append(" MAXVALUE ").Append(Number(max));

        return sql.ToString();
    }

    /// <summary>Composes the <c>CREATE SEQUENCE</c> of a sequence in the EF model.</summary>
    internal static string Create(IReadOnlySequence sequence, bool ifNotExists)
        => Create(
            sequence.Name,
            sequence.Type,
            sequence.StartValue,
            sequence.IncrementBy,
            sequence.MinValue,
            sequence.MaxValue,
            sequence.IsCyclic,
            ifNotExists);

    /// <summary>
    /// The column default that draws from <paramref name="sequenceName"/>. The name is a string
    /// argument, not an identifier, so it is quoted as a literal.
    /// </summary>
    internal static string NextValue(string sequenceName)
    {
        CamusSqlSyntax.ValidateIdentifier(sequenceName, nameof(sequenceName));

        return $"nextval({CamusSqlSyntax.Literal(sequenceName, $"sequence name '{sequenceName}'")})";
    }

    /// <summary>The statement that draws one value: a <c>SELECT</c> with no <c>FROM</c>, which the server accepts.</summary>
    internal static string SelectNextValue(string sequenceName) => "SELECT " + NextValue(sequenceName);

    internal static void EnsureSupported(string name, Type clrType, bool isCyclic)
    {
        if (isCyclic)
            throw new NotSupportedException(
                $"CamusDB does not support cyclic sequences. Sequence '{name}' is configured with IsCyclic(true). " +
                "A CamusDB sequence value is unique for the life of the sequence. Remove IsCyclic, or raise the maximum.");

        if (!IsSupportedType(clrType))
            throw new NotSupportedException(
                $"CamusDB sequences store int64 values. Sequence '{name}' has type '{clrType.Name}'; " +
                "use long, int, short or byte.");
    }

    internal static string Number(long value) => value.ToString(CultureInfo.InvariantCulture);
}
