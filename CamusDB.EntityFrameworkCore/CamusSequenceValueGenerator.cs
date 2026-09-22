/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Globalization;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.ValueGeneration;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Gives a new entity its value from a CamusDB sequence with <c>SELECT nextval('…')</c>, when the
/// entity is added to the context.
/// </summary>
/// <remarks>
/// <para><b>Why the value is drawn on the client.</b> CamusDB has no <c>RETURNING</c> clause, so EF
/// cannot read back a value that a column default generated on the server. The generator draws the
/// value first and EF sends it in the <c>INSERT</c>. The entity then has its real value before
/// <c>SaveChanges</c>, and a key that uses it can be referenced at once.</para>
///
/// <para><b>Blocks.</b> With a block size of 1 (<c>UseSequence</c>), each value costs one round trip.
/// With a block size of <c>n</c> (<c>UseHiLo</c>), the sequence has <c>INCREMENT BY n</c>. Each
/// <c>nextval</c> result <c>v</c> then reserves <c>v … v + n − 1</c> for this process, and the next
/// <c>n − 1</c> values need no round trip. The blocks of two processes cannot overlap, because every
/// block starts at a value that the sequence issued once only. A block does not go past the maximum of
/// the sequence. The values that a process does not use are lost, which is a gap, not a duplicate.</para>
///
/// <para>A block is kept for each database, not for each context. It is the same rule the SQL Server
/// provider uses: a block drawn from database A must never supply a row in database B.</para>
///
/// <para><b>Transactions.</b> The draw runs on the context's connection, in its current transaction if
/// there is one. A rollback does not return the values: the server never reissues a sequence value.</para>
/// </remarks>
public sealed class CamusSequenceValueGenerator<TValue> : ValueGenerator<TValue>
{
    private readonly string _sequenceName;
    private readonly string _sql;
    private readonly int _blockSize;
    private readonly long? _maxValue;
    private readonly ConcurrentDictionary<string, Block> _blocks = new(StringComparer.Ordinal);

    /// <param name="sequenceName">The sequence to draw from.</param>
    /// <param name="blockSize">How many values one <c>nextval</c> reserves: the increment of the sequence.</param>
    /// <param name="maxValue">The maximum of the sequence, or <see langword="null"/> for none. A block stops there.</param>
    public CamusSequenceValueGenerator(string sequenceName, int blockSize = 1, long? maxValue = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(blockSize, 1);

        _sequenceName = sequenceName;
        _sql = CamusSequenceSyntax.SelectNextValue(sequenceName);
        _blockSize = blockSize;
        _maxValue = maxValue;
    }

    /// <inheritdoc />
    public override bool GeneratesTemporaryValues => false;

    /// <inheritdoc />
    public override TValue Next(EntityEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);

        Block? block = _blockSize > 1 ? GetBlock(entry.Context) : null;
        if (block?.TryTake(out long cached) == true)
            return Convert(cached);

        long value = ToInt64(BuildCommand(entry.Context, out RelationalCommandParameterObject parameters).ExecuteScalar(parameters));
        return Convert(Refill(block, value));
    }

    /// <inheritdoc />
    public override async ValueTask<TValue> NextAsync(EntityEntry entry, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(entry);

        Block? block = _blockSize > 1 ? GetBlock(entry.Context) : null;
        if (block?.TryTake(out long cached) == true)
            return Convert(cached);

        IRelationalCommand command = BuildCommand(entry.Context, out RelationalCommandParameterObject parameters);
        long value = ToInt64(await command.ExecuteScalarAsync(parameters, cancellationToken).ConfigureAwait(false));
        return Convert(Refill(block, value));
    }

    private IRelationalCommand BuildCommand(DbContext context, out RelationalCommandParameterObject parameters)
    {
        parameters = new RelationalCommandParameterObject(
            context.GetService<IRelationalConnection>(),
            parameterValues: null,
            readerColumns: null,
            context,
            context.GetService<IRelationalCommandDiagnosticsLogger>(),
            CommandSource.ValueGenerator);

        return context.GetService<IRawSqlCommandBuilder>().Build(_sql);
    }

    private Block GetBlock(DbContext context)
    {
        var connection = context.GetService<IRelationalConnection>().DbConnection;

        return _blocks.GetOrAdd(connection.DataSource + "\0" + connection.Database, static _ => new Block());
    }

    /// <summary>
    /// Returns <paramref name="value"/> and keeps the rest of its block for the next calls. When another
    /// thread refilled the block first, the rest of this block is discarded, which leaves a gap only.
    /// </summary>
    private long Refill(Block? block, long value)
    {
        if (block is null)
            return value;

        long last = value > long.MaxValue - (_blockSize - 1) ? long.MaxValue : value + (_blockSize - 1);
        if (_maxValue is { } max && last > max)
            last = max;

        if (last > value)
            block.Offer(value + 1, last);

        return value;
    }

    private long ToInt64(object? result)
        => result is null or DBNull
            ? throw new InvalidOperationException($"nextval('{_sequenceName}') returned no value.")
            : System.Convert.ToInt64(result, CultureInfo.InvariantCulture);

    private TValue Convert(long value)
    {
        Type target = Nullable.GetUnderlyingType(typeof(TValue)) ?? typeof(TValue);

        try
        {
            return (TValue)System.Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
        }
        catch (OverflowException ex)
        {
            throw new InvalidOperationException(
                $"Sequence '{_sequenceName}' issued {value}, which does not fit in {target.Name}. " +
                $"Set a maximum on the sequence (HasMax), or map the property as long.", ex);
        }
    }

    /// <summary>The unused part of the last block: the values <c>next … last</c>.</summary>
    private sealed class Block
    {
        private readonly object _lock = new();
        private long _next = 1;
        private long _last;

        public bool TryTake(out long value)
        {
            lock (_lock)
            {
                if (_next <= _last)
                {
                    value = _next++;
                    return true;
                }
            }

            value = 0;
            return false;
        }

        public void Offer(long next, long last)
        {
            lock (_lock)
            {
                if (_next > _last)
                {
                    _next = next;
                    _last = last;
                }
            }
        }
    }
}
