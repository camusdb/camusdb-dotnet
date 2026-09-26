
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Stamps a fresh, strictly-increasing byte[] row-version token onto every Added and Modified entity
/// before <c>SaveChanges</c>. This provides the value CamusDB will not generate server-side. Because
/// the token is a concurrency token, EF writes the new value in the UPDATE's SET while matching the
/// previously-loaded value in the WHERE — a stale write matches zero rows and raises
/// <see cref="DbUpdateConcurrencyException"/>.
/// </summary>
public sealed class CamusRowVersionInterceptor : ISaveChangesInterceptor
{
    private static long _last;

    public InterceptionResult<int> SavingChanges(
        DbContextEventData eventData, InterceptionResult<int> result)
    {
        Stamp(eventData.Context);
        return result;
    }

    public ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        Stamp(eventData.Context);
        return ValueTask.FromResult(result);
    }

    // The row-version properties of each entity type, and whether a model has any. Both are fixed once a
    // model is built, so they are found once per model instead of by a scan of every property of every
    // saved entry. The tables hold the model weakly, so a discarded model is not kept alive.
    private static readonly ConditionalWeakTable<IEntityType, IProperty[]> RowVersionProperties = new();

    private static readonly ConditionalWeakTable<IModel, StrongBox<bool>> ModelHasRowVersions = new();

    private static void Stamp(DbContext? context)
    {
        if (context is null)
            return;

        // A model with no row-version property has nothing to stamp. Entries() is skipped too: it only
        // runs DetectChanges, which SaveChanges runs itself.
        if (!ModelHasRowVersions.GetValue(context.Model, static model => new StrongBox<bool>(
                model.GetEntityTypes().Any(entityType => FindRowVersionProperties(entityType).Length > 0))).Value)
            return;

        foreach (var entry in context.ChangeTracker.Entries())
        {
            if (entry.State != EntityState.Added && entry.State != EntityState.Modified)
                continue;

            foreach (IProperty property in FindRowVersionProperties(entry.Metadata))
                entry.Property(property).CurrentValue = NextToken();
        }
    }

    private static IProperty[] FindRowVersionProperties(IEntityType entityType)
        => RowVersionProperties.GetValue(entityType, static type => type.GetProperties()
            .Where(property => property.ClrType == typeof(byte[])
                && property.IsConcurrencyToken
                && property.ValueGenerated == ValueGenerated.OnAddOrUpdate)
            .ToArray());

    private static byte[] NextToken()
    {
        long now = DateTime.UtcNow.Ticks;
        long ticks;
        lock (typeof(CamusRowVersionInterceptor))
        {
            ticks = now > _last ? now : _last + 1;
            _last = ticks;
        }

        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(bytes, ticks);
        return bytes;
    }
}
