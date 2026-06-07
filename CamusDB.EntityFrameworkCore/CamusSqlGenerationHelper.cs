using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

public class CamusSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public CamusSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies) { }

    // CamusDB uses bare (unquoted) identifiers — no " wrapping
    public override string DelimitIdentifier(string identifier) => identifier;
    public override void DelimitIdentifier(StringBuilder builder, string identifier) => builder.Append(identifier);
    public override string DelimitIdentifier(string name, string? schema) => name;
    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema) => builder.Append(name);

    // CamusDB doesn't use statement terminators
    public override string StatementTerminator => "";
}
