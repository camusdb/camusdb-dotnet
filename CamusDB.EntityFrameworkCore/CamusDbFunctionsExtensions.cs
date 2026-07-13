
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
/// LINQ query and translated to CamusDB's native <c>regexp_*</c> scalar functions. Every method here is a
/// translation stub: it throws if evaluated in memory, so it must only appear inside an expression tree
/// that the provider translates to SQL.
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

    private static InvalidOperationException ClientEval(string name) => new(
        $"EF.Functions.{name} is a CamusDB SQL translation and cannot be evaluated in memory; " +
        "it can only be used inside a LINQ query that is translated to SQL.");
}
