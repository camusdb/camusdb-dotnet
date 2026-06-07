using System.Text;
using Microsoft.EntityFrameworkCore.Storage;

namespace CamusDB.EntityFrameworkCore;

public class CamusSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public CamusSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies)
        : base(dependencies) { }

    public override string DelimitIdentifier(string identifier) => $"`{EscapeIdentifier(identifier)}`";

    public override void DelimitIdentifier(StringBuilder builder, string identifier)
    {
        builder.Append('`');
        EscapeIdentifier(builder, identifier);
        builder.Append('`');
    }

    public override string DelimitIdentifier(string name, string? schema) => DelimitIdentifier(name);

    public override void DelimitIdentifier(StringBuilder builder, string name, string? schema)
        => DelimitIdentifier(builder, name);

    public override string StatementTerminator => "";
}
