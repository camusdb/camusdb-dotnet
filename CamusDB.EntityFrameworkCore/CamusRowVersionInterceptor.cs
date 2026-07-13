
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Buffers.Binary;
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

    private static void Stamp(DbContext? context)
    {
        if (context is null)
            return;

        foreach (var entry in context.ChangeTracker.Entries())
        {
            if (entry.State != EntityState.Added && entry.State != EntityState.Modified)
                continue;

            foreach (var property in entry.Metadata.GetProperties())
            {
                if (property.ClrType == typeof(byte[])
                    && property.IsConcurrencyToken
                    && property.ValueGenerated == ValueGenerated.OnAddOrUpdate)
                {
                    entry.Property(property.Name).CurrentValue = NextToken();
                }
            }
        }
    }

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
