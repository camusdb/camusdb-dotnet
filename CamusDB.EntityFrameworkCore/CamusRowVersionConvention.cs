
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Builders;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace CamusDB.EntityFrameworkCore;

/// <summary>
/// CamusDB has no server-generated row version, so a <c>[Timestamp]</c> / <c>IsRowVersion()</c> byte[]
/// token is generated and sent by the provider (see <see cref="CamusRowVersionInterceptor"/>). By
/// default EF treats a row version as store-generated (<c>BeforeSaveBehavior = Ignore</c>), which would
/// omit it from INSERT/UPDATE. This convention flips those byte[] tokens to be written on both insert
/// and update while keeping them concurrency tokens (EF still puts the original value in the WHERE).
/// </summary>
public sealed class CamusRowVersionConvention : IModelFinalizingConvention
{
    public void ProcessModelFinalizing(
        IConventionModelBuilder modelBuilder,
        IConventionContext<IConventionModelBuilder> context)
    {
        foreach (var entityType in modelBuilder.Metadata.GetEntityTypes())
        {
            foreach (var property in entityType.GetDeclaredProperties())
            {
                if (property.ClrType == typeof(byte[])
                    && property.IsConcurrencyToken
                    && property.ValueGenerated == ValueGenerated.OnAddOrUpdate)
                {
                    // Send the provider-generated token on insert and update instead of ignoring it.
                    property.SetBeforeSaveBehavior(PropertySaveBehavior.Save);
                    property.SetAfterSaveBehavior(PropertySaveBehavior.Save);
                }
            }
        }
    }
}
