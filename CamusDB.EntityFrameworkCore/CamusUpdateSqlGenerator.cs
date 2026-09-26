using System.Text;
using Microsoft.EntityFrameworkCore.Update;

namespace CamusDB.EntityFrameworkCore;

public class CamusUpdateSqlGenerator : UpdateSqlGenerator
{
    public CamusUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies)
        : base(dependencies) { }

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

        commandStringBuilder.AppendLine(")");
        return ResultSetMapping.NoResults;
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
