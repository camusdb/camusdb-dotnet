
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using CamusDB.Client.Auth;
using CamusDB.Client.Transport;
using Flurl.Http;

namespace CamusDB.Client;

/// <summary>
/// The node's backup administration API — the <b>online</b> half of CamusDB's backup and
/// point-in-time-recovery surface: taking backups, listing the catalog, resolving and validating a
/// restore chain, and running retention. All of these are safe while the server serves traffic.
///
/// <para><b>Not an ADO surface.</b> Backups are node-wide (every database on a server shares one Kahuna
/// node) and server-level, so nothing here is scoped to the connection's <c>Database=</c>; a backup taken
/// through one connection captures every database on that node. Reach it through
/// <see cref="CamusConnection.Backups"/>.</para>
///
/// <para><b>Restore is deliberately absent.</b> <c>POST /v1/restore</c> rebuilds into a <i>fresh</i> data
/// root and the operator must then stop the server and boot a new one against it — there is no hot
/// in-place restore, so it is not an operation an application can drive to completion. It stays an
/// operator runbook step (see the <c>backups-and-point-in-time-recovery</c> guide), not a driver call.</para>
///
/// <para><b>Transport.</b> These endpoints exist only as REST/JSON on the server's HTTP port: they have
/// no SQL form and no gRPC service, so unlike <c>CREATE DATABASE</c> and friends they cannot be composed
/// as SQL over the gRPC transport. This client therefore always speaks HTTP, independently of the
/// connection's <c>Protocol=</c>. A gRPC connection must say where the HTTP port is via
/// <c>BackupEndpoint=</c>; see <see cref="CamusConnectionStringBuilder.GetBackupEndpoint"/>.</para>
///
/// <para><b>Authorization.</b> With authentication enabled every call needs a <i>superuser</i> bearer
/// token — the connection's own token is used, so authenticate with a superuser account. With
/// authentication disabled the server restricts this surface to loopback callers, so a remote client
/// gets <c>CADB0517</c> rather than an anonymous node-wide backup.</para>
/// </summary>
public sealed class CamusBackupClient
{
    private readonly CamusConnectionStringBuilder builder;

    private readonly CamusTokenProvider auth;

    internal CamusBackupClient(CamusConnectionStringBuilder builder, CamusTokenProvider auth)
    {
        this.builder = builder;
        this.auth = auth;
    }

    /// <summary>
    /// Takes a full backup of the node now: a complete base image that restores on its own, with no
    /// parent to chain onto.
    /// </summary>
    public Task<CamusBackupInfo> TakeFullBackupAsync(CancellationToken cancellationToken = default)
        => TakeBackupAsync(["v1", "backups", "full"], body: null, "Take full backup failed", cancellationToken);

    /// <summary>
    /// Takes an incremental backup on top of <paramref name="parentBackupId"/>, capturing only what
    /// changed since it.
    ///
    /// <para>If the parent has aged past the retention floor the server cannot produce a contiguous
    /// increment and transparently takes a <i>full</i> backup instead. That substitution is visible rather
    /// than silent: the returned <see cref="CamusBackupInfo.WasSubstituted"/> is true and
    /// <see cref="CamusBackupInfo.SubstitutionReason"/> explains it. The call still succeeds.</para>
    /// </summary>
    /// <param name="parentBackupId">The backup to chain onto — a non-empty GUID, typically the
    /// <see cref="CamusBackupInfo.BackupId"/> of an earlier full or incremental backup.</param>
    public Task<CamusBackupInfo> TakeIncrementalBackupAsync(string parentBackupId, CancellationToken cancellationToken = default)
    {
        CamusTakeBackupRequest request = new() { ParentBackupId = parentBackupId };

        return TakeBackupAsync(
            ["v1", "backups", "incremental"],
            CamusJsonContent.Create(request, CamusJsonSerializerContext.Default.CamusTakeBackupRequest),
            "Take incremental backup failed",
            cancellationToken);
    }

    /// <inheritdoc cref="TakeIncrementalBackupAsync(string, CancellationToken)"/>
    public Task<CamusBackupInfo> TakeIncrementalBackupAsync(Guid parentBackupId, CancellationToken cancellationToken = default)
        => TakeIncrementalBackupAsync(parentBackupId.ToString(), cancellationToken);

    /// <summary>
    /// Takes a cluster-wide coordinated full backup: one consistent HLC cut across every partition, so a
    /// cross-partition transaction cannot be torn. The production recommendation for clusters; on a single
    /// embedded node it is effectively equivalent to <see cref="TakeFullBackupAsync"/>.
    ///
    /// <para>Must be issued against the <b>coordinator</b> node — another node refuses with
    /// <c>BackupNotCoordinator</c>. Pin <c>BackupEndpoint=</c> to the coordinator when the connection's
    /// <c>Endpoint=</c> is a multi-node pool. If the topology changes mid-backup the server aborts with
    /// <c>BackupTopologyChanged</c> and publishes nothing, so retrying once the cluster is stable is
    /// safe.</para>
    /// </summary>
    public Task<CamusBackupInfo> TakeCoordinatedBackupAsync(CancellationToken cancellationToken = default)
        => TakeBackupAsync(["v1", "backups", "coordinated"], body: null, "Take coordinated backup failed", cancellationToken);

    /// <summary>
    /// Lists every backup in the node's catalog.
    ///
    /// <para>An entry whose manifest could not be read is still reported, with
    /// <see cref="CamusBackupInfo.IsInvalid"/> set — listing fails open on a single bad manifest so one
    /// corrupt entry never hides the rest of the catalog.</para>
    /// </summary>
    public Task<IReadOnlyList<CamusBackupInfo>> ListBackupsAsync(CancellationToken cancellationToken = default)
        => ListAsync(["v1", "backups"], "List backups failed", cancellationToken);

    /// <summary>
    /// Resolves and validates the restore chain ending at <paramref name="leafBackupId"/>, returned
    /// root-first (the full base image, then each incremental in order).
    ///
    /// <para>This is the validating read: a chain that cannot be assembled is rejected here — with
    /// <c>BackupChainInvalid</c> or <c>BackupCorruptArtifact</c> — rather than at restore time, so it
    /// doubles as a "would this backup actually restore?" check.</para>
    ///
    /// <para>The chain's recoverable coverage is reported on the <b>first</b> element (the root), not the
    /// leaf: its <see cref="CamusBackupInfo.MinRecoverablePhysicalMs"/> and
    /// <see cref="CamusBackupInfo.MaxRecoverablePhysicalMs"/> are the exact window a point-in-time restore
    /// may target, and a <c>targetTimeMs</c> outside it is refused with <c>RestorePointOutOfWindow</c>.
    /// Recoverability is a property of the chain, not of elapsed wall-clock time, so an archived backup
    /// stays restorable.</para>
    /// </summary>
    public Task<IReadOnlyList<CamusBackupInfo>> GetChainAsync(string leafBackupId, CancellationToken cancellationToken = default)
        => ListAsync(["v1", "backups", leafBackupId, "chain"], "Get backup chain failed", cancellationToken);

    /// <inheritdoc cref="GetChainAsync(string, CancellationToken)"/>
    public Task<IReadOnlyList<CamusBackupInfo>> GetChainAsync(Guid leafBackupId, CancellationToken cancellationToken = default)
        => GetChainAsync(leafBackupId.ToString(), cancellationToken);

    /// <summary>
    /// Previews a retention pass without deleting anything: what the configured
    /// <c>backup_retention_max_chains</c> / <c>_max_age_seconds</c> / <c>_max_bytes</c> limits would
    /// reclaim right now. The returned <see cref="CamusBackupGcResult.Applied"/> is false.
    /// </summary>
    public Task<CamusBackupGcResult> PreviewGarbageCollectionAsync(CancellationToken cancellationToken = default)
        => RunGarbageCollectionAsync(dryRun: true, cancellationToken);

    /// <summary>
    /// Runs backup retention and the orphan sweep on demand, deleting what
    /// <see cref="PreviewGarbageCollectionAsync"/> reports.
    ///
    /// <para>Rarely needed: retention runs automatically after each backup and on a periodic tick. Use
    /// this to reclaim space immediately after tightening the limits. Only whole chains are deleted, and
    /// a valid full root is always left for every retained leaf.</para>
    /// </summary>
    public Task<CamusBackupGcResult> CollectGarbageAsync(CancellationToken cancellationToken = default)
        => RunGarbageCollectionAsync(dryRun: false, cancellationToken);

    private async Task<CamusBackupInfo> TakeBackupAsync(
        string[] segments, HttpContent? body, string failureMessage, CancellationToken cancellationToken)
    {
        string endpoint = builder.GetBackupEndpoint();

        try
        {
            byte[] responseBytes = await (await AuthorizeAsync(endpoint, cancellationToken).ConfigureAwait(false))
                .WithTimeout(builder.BackupTimeout)
                .AppendPathSegments(segments)
                .PostAsync(body, cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusBackupResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusBackupResponse);

            if (response?.Status != "ok" || response.Backup is null)
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? failureMessage);

            return response.Backup;
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    private async Task<IReadOnlyList<CamusBackupInfo>> ListAsync(
        string[] segments, string failureMessage, CancellationToken cancellationToken)
    {
        string endpoint = builder.GetBackupEndpoint();

        try
        {
            byte[] responseBytes = await (await AuthorizeAsync(endpoint, cancellationToken).ConfigureAwait(false))
                .WithTimeout(builder.BackupTimeout)
                .AppendPathSegments(segments)
                .GetBytesAsync(cancellationToken: cancellationToken);

            CamusBackupListResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusBackupListResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? failureMessage);

            return (IReadOnlyList<CamusBackupInfo>?)response.Backups ?? [];
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    private async Task<CamusBackupGcResult> RunGarbageCollectionAsync(bool dryRun, CancellationToken cancellationToken)
    {
        string endpoint = builder.GetBackupEndpoint();

        try
        {
            byte[] responseBytes = await (await AuthorizeAsync(endpoint, cancellationToken).ConfigureAwait(false))
                .WithTimeout(builder.BackupTimeout)
                .AppendPathSegments("v1", "backups", "gc")
                .SetQueryParam("dryRun", dryRun ? "true" : "false")
                .PostAsync(null, cancellationToken: cancellationToken)
                .ReceiveBytes();

            CamusBackupGcResponse? response = JsonSerializer.Deserialize(responseBytes, CamusJsonSerializerContext.Default.CamusBackupGcResponse);

            if (response?.Status != "ok")
                throw new CamusException(response?.Code ?? "CADB0000", response?.Message ?? "Backup garbage collection failed");

            return new CamusBackupGcResult
            {
                Applied = response.Applied,
                BytesReclaimed = response.BytesReclaimed,
                RetentionDeletions = response.RetentionDeletions,
                OrphanReclamations = response.OrphanReclamations
            };
        }
        catch (FlurlHttpException ex)
        {
            throw await TranslateAsync(ex, endpoint).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Attaches the connection's bearer token, if it has one. Mirrors <see cref="RestTransport"/>: every
    /// route is built here, so none can accidentally go out unauthenticated, and when no credentials are
    /// configured no <c>Authorization</c> header is added at all — which is what a server with
    /// authentication off expects.
    /// </summary>
    private async Task<IFlurlRequest> AuthorizeAsync(string endpoint, CancellationToken cancellationToken)
    {
        IFlurlRequest request = endpoint.WithHeader("Accept", "application/json");

        string? token = await auth.GetTokenAsync(cancellationToken).ConfigureAwait(false);

        return token is null ? request : request.WithOAuthBearerToken(token);
    }

    private async Task<CamusException> TranslateAsync(FlurlHttpException ex, string endpoint)
    {
        CamusEndpointHealth.MarkUnreachableIfTransportFailed(builder, endpoint, ex);

        return await RestErrorTranslator.TranslateAsync(ex).ConfigureAwait(false);
    }
}
