/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

namespace OpenTelemetry.Metrics;

/// <summary>Registers the CamusDB.Client metrics with a <see cref="MeterProviderBuilder"/>.</summary>
public static class CamusMeterProviderBuilderExtensions
{
    /// <summary>
    /// Collects the metrics of the CamusDB driver: <c>db.client.operation.duration</c> and the
    /// <c>camusdb.client.*</c> retry, endpoint and prepared-statement metrics. The same as
    /// <c>AddMeter(CamusTelemetry.MeterName)</c>.
    /// </summary>
    public static MeterProviderBuilder AddCamusDBInstrumentation(this MeterProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.AddMeter(CamusTelemetry.MeterName);
    }
}
