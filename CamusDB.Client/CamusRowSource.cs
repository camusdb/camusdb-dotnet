
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDB.Client;

/// <summary>
/// The row backing behind a <see cref="CamusDataReader"/>. Abstracts <b>where</b> the reader's rows come
/// from so every <c>Get*</c> accessor (which reads the current row's cells positionally) is written once
/// and works for both delivery modes:
///
/// <list type="bullet">
///   <item><see cref="Buffered"/> — the whole result set is already in memory (a <see cref="CamusResultSet"/>),
///     the historical behavior of the <c>/execute-sql-query</c> endpoint and of DML affected-row results.</item>
///   <item>The streaming NDJSON source (<c>Transport/NdjsonStreamRowSource</c>) — rows are pulled from the
///     network one line at a time as the reader advances, so a multi-thousand-row / multi-MB result never
///     materializes client-side. Backs <see cref="CamusCommand.ExecuteStreamReaderAsync()"/> over the
///     <c>/execute-sql-query-stream</c> endpoint.</item>
/// </list>
///
/// The schema (column names + declared types) is always known up front — the streaming source reads the
/// NDJSON header line before the reader sees a row — so field count / names / types are reportable before
/// (and independent of) the first <see cref="Read"/>.
/// </summary>
internal abstract class CamusRowSource : IDisposable, IAsyncDisposable
{
    /// <summary>Output column names, positionally aligned with <see cref="ColumnTypes"/>.</summary>
    public abstract string[] ColumnNames { get; }

    /// <summary>Declared column types, or null when the shape is only knowable from the current row.</summary>
    public abstract ColumnType[]? ColumnTypes { get; }

    /// <summary>True when the result has at least one row (known up front, even before the first read).</summary>
    public abstract bool HasRows { get; }

    /// <summary>Affected-row count for a DML reader, or -1 for a query reader.</summary>
    public virtual int RecordsAffected => -1;

    /// <summary>Advances to the next row synchronously. Returns false at end of result.</summary>
    public abstract bool Read();

    /// <summary>Advances to the next row. Returns false at end of result.</summary>
    public abstract ValueTask<bool> ReadAsync(CancellationToken cancellationToken);

    /// <summary>The current row's cell at <paramref name="ordinal"/>. Throws when positioned off a row.</summary>
    public abstract ColumnValue GetCell(int ordinal);

    public virtual void Dispose() { }

    public virtual ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>A fully-materialized source over a <see cref="CamusResultSet"/> (or a DML row count).</summary>
    internal static CamusRowSource Buffered(CamusResultSet resultSet, int recordsAffected = -1)
        => new BufferedRowSource(resultSet, recordsAffected);

    private sealed class BufferedRowSource(CamusResultSet resultSet, int recordsAffected) : CamusRowSource
    {
        private int position = -1;

        public override string[] ColumnNames => resultSet.ColumnNames;

        public override ColumnType[]? ColumnTypes => resultSet.ColumnTypes;

        public override bool HasRows => resultSet.RowCount > 0;

        public override int RecordsAffected => recordsAffected;

        public override bool Read()
        {
            position++;
            return position < resultSet.RowCount;
        }

        public override ValueTask<bool> ReadAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return new ValueTask<bool>(Read());
        }

        public override ColumnValue GetCell(int ordinal)
        {
            if (position < 0 || position >= resultSet.RowCount)
                throw new InvalidOperationException("No current row is available.");

            return resultSet.GetCell(position, ordinal);
        }
    }
}
