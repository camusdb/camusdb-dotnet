/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

namespace OpenTelemetry.Trace;

/// <summary>
/// Registers the CamusDB.Client spans with a <see cref="TracerProviderBuilder"/>. The extension is in the
/// <c>OpenTelemetry.Trace</c> namespace, as the extensions of other instrumentation libraries are, so it is
/// in scope where the tracer provider is configured.
/// </summary>
public static class CamusTracerProviderBuilderExtensions
{
    /// <summary>
    /// Collects the spans of the CamusDB driver: one client span per command, <c>BEGIN</c>,
    /// <c>COMMIT</c>, <c>ROLLBACK</c> and pipeline exchange. The same as
    /// <c>AddSource(CamusTelemetry.ActivitySourceName)</c>.
    /// </summary>
    public static TracerProviderBuilder AddCamusDBInstrumentation(this TracerProviderBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        return builder.AddSource(CamusTelemetry.ActivitySourceName);
    }
}
