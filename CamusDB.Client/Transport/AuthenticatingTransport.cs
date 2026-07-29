/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client.Auth;

namespace CamusDB.Client.Transport;

/// <summary>
/// Wraps a transport with the reactive half of token management. The inner transport attaches whatever
/// token <see cref="CamusTokenProvider"/> currently holds; this decorator watches for the server
/// rejecting it (<c>CADB0516</c>), discards it, and replays the operation once with a freshly minted one.
///
/// <para>That covers the failures a client-side expiry timer cannot see — a password rotation, a
/// <c>DROP USER</c>, a <c>/logout</c> from elsewhere, a server configured with a shorter
/// <c>AccessTokenTtl</c> than the driver assumes, or a token minted before a server restart. It does
/// <b>not</b> retry <c>CADB0517</c> (insufficient privilege): re-authenticating as the same user cannot
/// grant a privilege, so retrying would only double the work before the same denial.</para>
///
/// <para>Exactly one replay per call, and only when the attempt actually presented a token the provider
/// can replace: a rejection with no token in hand came from the login itself (wrong password), and
/// replaying it would burn two attempts per statement against a limit of twenty per account per minute.
/// A directly supplied <c>AccessToken=</c> is likewise surfaced as-is — there is no password to mint a
/// replacement with. Statements are replayed the same way the ADO layer already replays a serializable
/// conflict, so a replay is no less safe than the retry paths that were there before — a rejected request
/// never reached execution.</para>
/// </summary>
internal sealed class AuthenticatingTransport(ICamusTransport inner, CamusTokenProvider auth) : ICamusTransport, IDisposable
{
    public CamusProtocol Protocol => inner.Protocol;

    /// <summary>The wrapped transport, for tests and diagnostics that need the concrete implementation.</summary>
    public ICamusTransport Inner => inner;

    public Task<StartTransactionResult> StartTransactionAsync(
        string endpoint, string database, CamusTransactionOptions options, int timeoutSeconds, CancellationToken cancellationToken)
        => RunAsync(ct => inner.StartTransactionAsync(endpoint, database, options, timeoutSeconds, ct), cancellationToken);

    public Task FinalizeTransactionAsync(
        bool commit, string endpoint, string database, long txnIdPT, uint txnIdCounter, int? streamSlot, int timeoutSeconds, CancellationToken cancellationToken)
        => RunAsync(ct => inner.FinalizeTransactionAsync(commit, endpoint, database, txnIdPT, txnIdCounter, streamSlot, timeoutSeconds, ct), cancellationToken);

    public Task<QueryTransportResult> ExecuteQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
        => RunAsync(ct => inner.ExecuteQueryAsync(request, ct), cancellationToken);

    public Task<CamusRowSource> ExecuteQueryStreamAsync(TransportSqlRequest request, CancellationToken cancellationToken)
        => RunAsync(ct => inner.ExecuteQueryStreamAsync(request, ct), cancellationToken);

    public Task<int> ExecuteNonQueryAsync(TransportSqlRequest request, CancellationToken cancellationToken)
        => RunAsync(ct => inner.ExecuteNonQueryAsync(request, ct), cancellationToken);

    public Task<bool> ExecuteDdlAsync(TransportSqlRequest request, CancellationToken cancellationToken)
        => RunAsync(ct => inner.ExecuteDdlAsync(request, ct), cancellationToken);

    public Task<bool> PingAsync(string endpoint, int timeoutSeconds, CancellationToken cancellationToken)
        => RunAsync(ct => inner.PingAsync(endpoint, timeoutSeconds, ct), cancellationToken);

    public Task CreateDatabaseAsync(
        string endpoint, string database, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
        => RunAsync(ct => inner.CreateDatabaseAsync(endpoint, database, ifNotExists, timeoutSeconds, ct), cancellationToken);

    public Task CreateBranchDatabaseAsync(
        string endpoint, string branchName, string sourceDatabaseName, bool ifNotExists, int timeoutSeconds, CancellationToken cancellationToken)
        => RunAsync(ct => inner.CreateBranchDatabaseAsync(endpoint, branchName, sourceDatabaseName, ifNotExists, timeoutSeconds, ct), cancellationToken);

    public Task DropDatabaseAsync(string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
        => RunAsync(ct => inner.DropDatabaseAsync(endpoint, database, timeoutSeconds, ct), cancellationToken);

    public Task<IReadOnlyList<CamusBranchRow>> ShowBranchesAsync(
        string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
        => RunAsync(ct => inner.ShowBranchesAsync(endpoint, database, timeoutSeconds, ct), cancellationToken);

    public Task<IReadOnlyList<CamusBranchRow>> ShowAncestorsAsync(
        string endpoint, string database, int timeoutSeconds, CancellationToken cancellationToken)
        => RunAsync(ct => inner.ShowAncestorsAsync(endpoint, database, timeoutSeconds, ct), cancellationToken);

    private async Task<T> RunAsync<T>(Func<CancellationToken, Task<T>> operation, CancellationToken cancellationToken)
    {
        // Captured before the call so a concurrent refresh isn't thrown away: only the token this
        // attempt actually presented is discarded.
        string? presented = auth.CurrentToken;

        try
        {
            return await operation(cancellationToken).ConfigureAwait(false);
        }
        catch (CamusException ex) when (IsRenewable(ex, presented))
        {
            auth.Invalidate(presented);
        }

        return await operation(cancellationToken).ConfigureAwait(false);
    }

    private async Task RunAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken)
    {
        string? presented = auth.CurrentToken;

        try
        {
            await operation(cancellationToken).ConfigureAwait(false);
            return;
        }
        catch (CamusException ex) when (IsRenewable(ex, presented))
        {
            auth.Invalidate(presented);
        }

        await operation(cancellationToken).ConfigureAwait(false);
    }

    private bool IsRenewable(CamusException ex, string? presented)
        => presented is not null && ex.Code == CamusAuthErrorCodes.AuthenticationFailed && auth.CanRenew;

    public void Dispose() => (inner as IDisposable)?.Dispose();
}
