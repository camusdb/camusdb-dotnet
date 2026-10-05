using System.Text;
using Microsoft.EntityFrameworkCore.Update;

namespace CamusDB.EntityFrameworkCore;

public class CamusUpdateSqlGenerator : UpdateSqlGenerator
{
    public CamusUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies) { }

    /// <summary>
    /// Writes <c>INSERT INTO t (…) VALUES (…)</c>. When the entity has columns that the server
    /// generates (a column default, a sequence default from <c>UseSequence</c>, or any other
    /// store-generated value that the entity does not set), it adds <c>RETURNING</c> with those columns.
    /// EF then reads the stored values back into the entity, in the order of the read modifications.
    /// </summary>
    public override ResultSetMapping AppendInsertOperation(
        StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition,
        out bool requiresTransaction)
    {
        requiresTransaction = false;

        var modifications = command.ColumnModifications;
        if (!Any(modifications, IsInsertWrite))
            return ResultSetMapping.NoResults;

        commandStringBuilder.Append("INSERT INTO ")
            .Append(SqlGenerationHelper.DelimitIdentifier(command.TableName))
            .Append(" (");

        bool first = true;
        for (int i = 0; i < modifications.Count; i++)
        {
            var op = modifications[i];
            if (!IsInsertWrite(op)) continue;
            if (!first) commandStringBuilder.Append(", ");
            commandStringBuilder.Append(SqlGenerationHelper.DelimitIdentifier(op.ColumnName));
            first = false;
        }

        commandStringBuilder.Append(") VALUES (");

        first = true;
        for (int i = 0; i < modifications.Count; i++)
        {
            var op = modifications[i];
            if (!IsInsertWrite(op)) continue;
            if (!first) commandStringBuilder.Append(", ");
            if (op.UseCurrentValueParameter && op.ParameterName is not null)
                commandStringBuilder.Append(SqlGenerationHelper.GenerateParameterName(op.ParameterName));
            else
                commandStringBuilder.Append("NULL");
            first = false;
        }

        commandStringBuilder.Append(')');

        bool returns = AppendReturning(commandStringBuilder, modifications);

        commandStringBuilder.AppendLine();
        return returns ? ResultSetMapping.LastInResultSet : ResultSetMapping.NoResults;
    }

    public override ResultSetMapping AppendUpdateOperation(
        StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition,
        out bool requiresTransaction)
    {
        requiresTransaction = false;

        var modifications = command.ColumnModifications;
        if (!Any(modifications, IsUpdateWrite))
            return ResultSetMapping.NoResults;

        commandStringBuilder.Append("UPDATE ")
            .Append(SqlGenerationHelper.DelimitIdentifier(command.TableName))
            .Append(" SET ");

        bool first = true;
        for (int i = 0; i < modifications.Count; i++)
        {
            var op = modifications[i];
            if (!IsUpdateWrite(op)) continue;
            if (!first) commandStringBuilder.Append(", ");
            commandStringBuilder.Append(SqlGenerationHelper.DelimitIdentifier(op.ColumnName)).Append(" = ");
            if (op.UseCurrentValueParameter && op.ParameterName is not null)
                commandStringBuilder.Append(SqlGenerationHelper.GenerateParameterName(op.ParameterName));
            else
                commandStringBuilder.Append("NULL");
            first = false;
        }

        AppendKeyConditions(commandStringBuilder, modifications);

        commandStringBuilder.AppendLine();
        return ResultSetMapping.NoResults;
    }

    public override ResultSetMapping AppendDeleteOperation(
        StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition,
        out bool requiresTransaction)
    {
        requiresTransaction = false;

        commandStringBuilder.Append("DELETE FROM ")
            .Append(SqlGenerationHelper.DelimitIdentifier(command.TableName));

        AppendKeyConditions(commandStringBuilder, command.ColumnModifications);

        commandStringBuilder.AppendLine();
        return ResultSetMapping.NoResults;
    }

    // The column filters, applied while the modifications are walked instead of copied into a list per
    // statement.
    private static bool IsInsertWrite(IColumnModification op) => op.IsWrite;

    private static bool IsUpdateWrite(IColumnModification op) => op.IsWrite && !op.IsKey;

    private static bool IsKeyCondition(IColumnModification op) => op.IsKey || op.IsCondition;

    /// <summary>
    /// Appends <c> RETURNING a, b</c> for the read modifications, and returns whether it appended one.
    /// The server has <c>RETURNING</c> on <c>INSERT</c> only, so an update never reads values back: a
    /// row version is stamped on the client, and the model refuses computed columns.
    /// </summary>
    private bool AppendReturning(StringBuilder commandStringBuilder, IReadOnlyList<IColumnModification> modifications)
    {
        bool first = true;
        for (int i = 0; i < modifications.Count; i++)
        {
            var op = modifications[i];
            if (!op.IsRead) continue;
            commandStringBuilder.Append(first ? " RETURNING " : ", ");
            commandStringBuilder.Append(SqlGenerationHelper.DelimitIdentifier(op.ColumnName));
            first = false;
        }

        return !first;
    }

    private static bool Any(IReadOnlyList<IColumnModification> modifications, Func<IColumnModification, bool> predicate)
    {
        for (int i = 0; i < modifications.Count; i++)
        {
            if (predicate(modifications[i]))
                return true;
        }

        return false;
    }

    private void AppendKeyConditions(StringBuilder commandStringBuilder, IReadOnlyList<IColumnModification> modifications)
    {
        bool first = true;
        for (int i = 0; i < modifications.Count; i++)
        {
            var op = modifications[i];
            if (!IsKeyCondition(op)) continue;
            commandStringBuilder.Append(first ? " WHERE " : " AND ");
            commandStringBuilder.Append(SqlGenerationHelper.DelimitIdentifier(op.ColumnName)).Append(" = ");
            var paramName = op.UseOriginalValueParameter ? op.OriginalParameterName : op.ParameterName;
            if (paramName is not null)
                commandStringBuilder.Append(SqlGenerationHelper.GenerateParameterName(paramName));
            else
                commandStringBuilder.Append("NULL");
            first = false;
        }
    }
}
