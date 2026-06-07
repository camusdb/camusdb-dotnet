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

        var writeOps = command.ColumnModifications.Where(o => o.IsWrite).ToList();
        if (writeOps.Count == 0)
            return ResultSetMapping.NoResults;

        commandStringBuilder.Append("INSERT INTO ").Append(command.TableName).Append(" (");

        bool first = true;
        foreach (var op in writeOps)
        {
            if (!first) commandStringBuilder.Append(", ");
            commandStringBuilder.Append(op.ColumnName);
            first = false;
        }

        commandStringBuilder.Append(") VALUES (");

        first = true;
        foreach (var op in writeOps)
        {
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

        var setOps = command.ColumnModifications.Where(o => o.IsWrite && !o.IsKey).ToList();
        var keyOps = command.ColumnModifications.Where(o => o.IsKey).ToList();

        if (setOps.Count == 0)
            return ResultSetMapping.NoResults;

        commandStringBuilder.Append("UPDATE ").Append(command.TableName).Append(" SET ");

        bool first = true;
        foreach (var op in setOps)
        {
            if (!first) commandStringBuilder.Append(", ");
            commandStringBuilder.Append(op.ColumnName).Append(" = ");
            if (op.UseCurrentValueParameter && op.ParameterName is not null)
                commandStringBuilder.Append(SqlGenerationHelper.GenerateParameterName(op.ParameterName));
            else
                commandStringBuilder.Append("NULL");
            first = false;
        }

        if (keyOps.Count > 0)
        {
            commandStringBuilder.Append(" WHERE ");
            first = true;
            foreach (var op in keyOps)
            {
                if (!first) commandStringBuilder.Append(" AND ");
                commandStringBuilder.Append(op.ColumnName).Append(" = ");
                var paramName = op.UseOriginalValueParameter ? op.OriginalParameterName : op.ParameterName;
                if (paramName is not null)
                    commandStringBuilder.Append(SqlGenerationHelper.GenerateParameterName(paramName));
                else
                    commandStringBuilder.Append("NULL");
                first = false;
            }
        }

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

        var keyOps = command.ColumnModifications.Where(o => o.IsKey).ToList();

        commandStringBuilder.Append("DELETE FROM ").Append(command.TableName);

        if (keyOps.Count > 0)
        {
            commandStringBuilder.Append(" WHERE ");
            bool first = true;
            foreach (var op in keyOps)
            {
                if (!first) commandStringBuilder.Append(" AND ");
                commandStringBuilder.Append(op.ColumnName).Append(" = ");
                var paramName = op.UseOriginalValueParameter ? op.OriginalParameterName : op.ParameterName;
                if (paramName is not null)
                    commandStringBuilder.Append(SqlGenerationHelper.GenerateParameterName(paramName));
                else
                    commandStringBuilder.Append("NULL");
                first = false;
            }
        }

        commandStringBuilder.AppendLine();
        return ResultSetMapping.NoResults;
    }
}
