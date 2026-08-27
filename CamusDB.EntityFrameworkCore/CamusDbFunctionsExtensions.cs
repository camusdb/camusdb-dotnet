
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.EntityFrameworkCore;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// CamusDB-specific <see cref="DbFunctions"/> extensions, invoked through <c>EF.Functions</c> inside a
/// LINQ query and translated to CamusDB's native <c>regexp_*</c> and vector scalar functions. Every method
/// here is a translation stub: it throws if evaluated in memory, so it must only appear inside an
/// expression tree that the provider translates to SQL.
///
/// <para>The optional <c>flags</c> string uses PostgreSQL-style option characters: <c>i</c> case-insensitive,
/// <c>c</c> case-sensitive, <c>m</c>/<c>n</c> multiline, <c>s</c> single-line (dot matches newline),
/// <c>x</c> extended (ignore pattern whitespace), and (where applicable) <c>g</c> global / replace-all.</para>
/// </summary>
public static class CamusDbFunctionsExtensions
{
    /// <summary><c>regexp_like(input, pattern)</c> — true when <paramref name="pattern"/> matches anywhere in <paramref name="input"/>. Equivalent to the <c>~</c> operator.</summary>
    public static bool RegexpLike(this DbFunctions _, string input, string pattern)
        => throw ClientEval(nameof(RegexpLike));

    /// <summary><c>regexp_like(input, pattern, flags)</c> — like <see cref="RegexpLike(DbFunctions, string, string)"/> with option flags (e.g. <c>"i"</c> for case-insensitive, the <c>~*</c> operator).</summary>
    public static bool RegexpLike(this DbFunctions _, string input, string pattern, string flags)
        => throw ClientEval(nameof(RegexpLike));

    /// <summary><c>regexp_replace(input, pattern, replacement)</c> — replaces the first match (PostgreSQL default). Pass <c>"g"</c> in the flags overload to replace all.</summary>
    public static string RegexpReplace(this DbFunctions _, string input, string pattern, string replacement)
        => throw ClientEval(nameof(RegexpReplace));

    /// <summary><c>regexp_replace(input, pattern, replacement, flags)</c> — replaces matches; include <c>"g"</c> to replace all occurrences.</summary>
    public static string RegexpReplace(this DbFunctions _, string input, string pattern, string replacement, string flags)
        => throw ClientEval(nameof(RegexpReplace));

    /// <summary><c>regexp_count(input, pattern)</c> — number of non-overlapping matches.</summary>
    public static int RegexpCount(this DbFunctions _, string input, string pattern)
        => throw ClientEval(nameof(RegexpCount));

    /// <summary><c>regexp_count(input, pattern, flags)</c> — number of non-overlapping matches with option flags.</summary>
    public static int RegexpCount(this DbFunctions _, string input, string pattern, string flags)
        => throw ClientEval(nameof(RegexpCount));

    /// <summary><c>regexp_substr(input, pattern)</c> — the first substring matching <paramref name="pattern"/>, or NULL when there is no match.</summary>
    public static string? RegexpSubstr(this DbFunctions _, string input, string pattern)
        => throw ClientEval(nameof(RegexpSubstr));

    /// <summary><c>regexp_substr(input, pattern, flags)</c> — the first matching substring with option flags.</summary>
    public static string? RegexpSubstr(this DbFunctions _, string input, string pattern, string flags)
        => throw ClientEval(nameof(RegexpSubstr));

    /// <summary><c>regexp_instr(input, pattern)</c> — the 1-based position of the first match, or 0 when there is no match.</summary>
    public static int RegexpInstr(this DbFunctions _, string input, string pattern)
        => throw ClientEval(nameof(RegexpInstr));

    /// <summary><c>regexp_instr(input, pattern, flags)</c> — the 1-based position of the first match with option flags.</summary>
    public static int RegexpInstr(this DbFunctions _, string input, string pattern, string flags)
        => throw ClientEval(nameof(RegexpInstr));

    // ── Vectors ───────────────────────────────────────────────────────────────────────────────────
    //
    // A vector is a bytes column holding tightly packed little-endian float32 elements with no header
    // (CamusDB.Client.CamusVector packs and unpacks that layout). All three distance functions take two
    // vectors of equal dimension and return float64. A dimension mismatch is an error, not a truncation.
    //
    // Search is exact: the server ranks every row the WHERE admits, so the cost is linear in
    // rows × dimensions. Always pair a distance ORDER BY with a Take(k) — that is what turns the sort
    // into a bounded top-k — and narrow the candidate set with a Where where you can.

    /// <summary>
    /// <c>l2_distance(a, b)</c> — Euclidean distance. The nearest vector is the one with the
    /// <em>smallest</em> value, so order ascending: <c>OrderBy(...).Take(10)</c>.
    /// </summary>
    public static double L2Distance(this DbFunctions _, byte[] a, byte[] b)
        => throw ClientEval(nameof(L2Distance));

    /// <summary>
    /// <c>cosine_distance(a, b)</c> — <c>1 - cosine_similarity</c>, so 0 for identical directions and 2
    /// for opposite ones, and never negative. The nearest vector is the one with the <em>smallest</em>
    /// value, so order ascending. A zero-magnitude vector is an error (<c>CADB0412</c>).
    /// </summary>
    public static double CosineDistance(this DbFunctions _, byte[] a, byte[] b)
        => throw ClientEval(nameof(CosineDistance));

    /// <summary>
    /// <c>inner_product(a, b)</c> — the dot product. This one runs the other way: the most similar vector
    /// is the one with the <em>largest</em> value, so order <b>descending</b> with
    /// <c>OrderByDescending(...)</c>. Ascending returns the least similar rows, and returns them without
    /// any error, so the mistake looks like a working query.
    /// </summary>
    public static double InnerProduct(this DbFunctions _, byte[] a, byte[] b)
        => throw ClientEval(nameof(InnerProduct));

    /// <summary>
    /// <c>vector_dims(v)</c> — the element count, <c>octet_length / 4</c>. A byte count that is not a
    /// multiple of four is rejected rather than rounded down (<c>CADB0410</c>), which is what makes this
    /// the right function for a dimension CHECK constraint.
    /// </summary>
    public static int VectorDims(this DbFunctions _, byte[] vector)
        => throw ClientEval(nameof(VectorDims));

    /// <summary>
    /// <c>octet_length(v)</c> — the byte count of a <c>bytes</c> value.
    /// </summary>
    public static int OctetLength(this DbFunctions _, byte[] value)
        => throw ClientEval(nameof(OctetLength));

    /// <summary>
    /// <c>octet_length(s)</c> — the <b>UTF-8</b> byte count of a string, which is not its length:
    /// <c>octet_length('áé')</c> is 4 while <c>length('áé')</c> is 2.
    /// </summary>
    public static int OctetLength(this DbFunctions _, string value)
        => throw ClientEval(nameof(OctetLength));

    private static InvalidOperationException ClientEval(string name) => new(
        $"EF.Functions.{name} is a CamusDB SQL translation and cannot be evaluated in memory; " +
        "it can only be used inside a LINQ query that is translated to SQL.");
}
