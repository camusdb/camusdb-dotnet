using System.Text;
using CamusDB.Client;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Identifier quoting for everything EF Core generates — every SELECT/INSERT/UPDATE/DELETE and all
/// migration DDL.
///
/// <para><b>Why <c>EscapeIdentifier</c> is overridden.</b> The relational base class escapes the double
/// quote, which is the delimiter it assumes. CamusDB delimits with backticks, so inheriting that
/// behavior would let a backtick inside a name pass through unescaped and end the quoting early. There
/// is no doubling that helps either — CamusDB trims the delimiters rather than decoding a doubled
/// backtick — so a name carrying one is refused, which is the same rule
/// <see cref="CamusIdentifier"/> applies to the statements the provider composes as text.</para>
/// </summary>
public class CamusSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public CamusSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies) { }

    public override string DelimitIdentifier(string identifier)
        => CamusSqlSyntax.DelimitIdentifier(identifier, nameof(identifier));

    public override void DelimitIdentifier(StringBuilder builder, string identifier)
    {
        CamusSqlSyntax.ValidateIdentifier(identifier, nameof(identifier));

        builder.Append('`').Append(identifier).Append('`');
    }

    public override string DelimitIdentifier(string name, string? schema) => DelimitIdentifier(name);

    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema)
        => DelimitIdentifier(builder, name);

    /// <inheritdoc />
    /// <remarks>Never reached through the <c>DelimitIdentifier</c> overrides above, which validate rather
    /// than escape; overridden so a base-class path that reaches it directly cannot apply the double-quote
    /// rule to a backtick-delimited identifier.</remarks>
    public override string EscapeIdentifier(string identifier)
    {
        CamusSqlSyntax.ValidateIdentifier(identifier, nameof(identifier));

        return identifier;
    }

    /// <inheritdoc />
    /// <remarks>See <see cref="EscapeIdentifier(string)"/>.</remarks>
    public override void EscapeIdentifier(StringBuilder builder, string identifier)
    {
        CamusSqlSyntax.ValidateIdentifier(identifier, nameof(identifier));

        builder.Append(identifier);
    }

    public override string StatementTerminator => "";
}
