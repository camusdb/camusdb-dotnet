/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Runtime.CompilerServices;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// Keeps <c>SaveChanges</c> from writing <c>NULL</c> over a stored vector that the entity never loaded.
///
/// <para>A vector that is not loaded with its entity (see the <c>loadWithEntity</c> parameter of
/// <see cref="CamusPropertyBuilderExtensions.IsVector(Microsoft.EntityFrameworkCore.Metadata.Builders.PropertyBuilder, int, bool)"/>)
/// is <see langword="null"/> on the entity. A tracked entity does not write it, because its original
/// value is <see langword="null"/> too. But <c>DbContext.Update</c> of a detached entity marks every
/// property as modified, and the <c>UPDATE</c> would then set the vector to <c>NULL</c>. This interceptor
/// clears the modified flag of such a vector when its value is <see langword="null"/>.</para>
///
/// <para>The model validator requires the property to be required, so <see langword="null"/> cannot be a
/// value that the user wants to store.</para>
/// </summary>
public sealed class CamusVectorSaveChangesInterceptor : ISaveChangesInterceptor
{
    public InterceptionResult<int> SavingChanges(
        DbContextEventData eventData, InterceptionResult<int> result)
    {
        KeepUnloadedVectors(eventData.Context);
        return result;
    }

    public ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        KeepUnloadedVectors(eventData.Context);
        return ValueTask.FromResult(result);
    }

    // The unloaded vector properties of each entity type, and whether a model has any. Both are fixed once
    // a model is built. The tables hold the model weakly, so a discarded model is not kept alive.
    private static readonly ConditionalWeakTable<IEntityType, IProperty[]> UnloadedVectorProperties = new();

    private static readonly ConditionalWeakTable<IModel, StrongBox<bool>> ModelHasUnloadedVectors = new();

    private static void KeepUnloadedVectors(DbContext? context)
    {
        if (context is null)
            return;

        if (!ModelHasUnloadedVectors.GetValue(context.Model, static model => new StrongBox<bool>(
                model.GetEntityTypes().Any(entityType => FindUnloadedVectors(entityType).Length > 0))).Value)
            return;

        foreach (var entry in context.ChangeTracker.Entries())
        {
            if (entry.State != EntityState.Modified)
                continue;

            foreach (IProperty property in FindUnloadedVectors(entry.Metadata))
            {
                var propertyEntry = entry.Property(property);
                if (propertyEntry.IsModified && propertyEntry.CurrentValue is null)
                    propertyEntry.IsModified = false;
            }
        }
    }

    private static IProperty[] FindUnloadedVectors(IEntityType entityType)
        => UnloadedVectorProperties.GetValue(entityType, static type => type.GetProperties()
            .Where(property => property.GetVectorDimensions() is not null && !property.IsLoadedWithEntity())
            .ToArray());
}
