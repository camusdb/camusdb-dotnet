using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("CamusDB.Client.Tests")]

// The EF Core provider composes DDL and migration text of its own, so it shares this assembly's SQL
// syntax rules (CamusSqlSyntax) rather than keeping a second copy of them that could drift.
[assembly: InternalsVisibleTo("CamusDB.EntityFrameworkCore")]
