/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace CamusDB.Client.Auth;

/// <summary>
/// Owns the connection's bearer token: mints it from the configured credentials on first use, hands the
/// same token to every request until it goes stale, and mints a new one when it does. One provider is
/// shared by every <see cref="CamusConnection"/> built from the same
/// <see cref="CamusConnectionStringBuilder"/>, so a pool of connections performs one login, not one per
/// connection — password verification is deliberately expensive server-side (PBKDF2, 600k iterations by
/// default) and rate-limited per account, so a login stampede is exactly what must be avoided.
///
/// <para><b>Single flight.</b> Concurrent callers that find no usable token queue on one gate; the
/// winner logs in and the rest observe its result instead of issuing their own login.</para>
///
/// <para><b>Renewal.</b> The <c>/login</c> reply reports how long the token is good for. At 80% of that
/// the provider starts one login in the background and keeps handing out the current token, so no
/// statement waits for a login it does not need yet; a statement waits only when there is no token, or
/// when the token reached 95% of its lifetime before a renewal succeeded. Against a server predating the
/// field it falls back to its own conservative lifetime (<c>TokenLifetime=</c>, default 10 minutes
/// against the server's 15-minute default <c>AccessTokenTtl</c>): it renews at 80% of that and never sends
/// a token past it. That proactive renewal is the primary path;
/// <see cref="Invalidate"/> — driven by a <c>CADB0516</c> from an actual request — is the backstop for
/// the cases a clock cannot predict: a password rotation, a <c>DROP USER</c>, a <c>/logout</c> elsewhere,
/// or a server whose TTL is configured shorter than the fallback.</para>
///
/// <para>A token supplied directly (<c>AccessToken=</c>) is used verbatim and never renewed — the driver
/// has no password to mint a replacement with, so a <c>CADB0516</c> on that path is surfaced to the
/// caller rather than retried.</para>
/// </summary>
internal sealed class CamusTokenProvider
{
    /// <summary>Token lifetime assumed when the server reports none, comfortably inside its 15-minute
    /// default TTL.</summary>
    public static readonly TimeSpan DefaultLifetime = TimeSpan.FromMinutes(10);

    // Providers keyed by the credentials they authenticate with — see Shared().
    private static readonly ConcurrentDictionary<string, CamusTokenProvider> SharedProviders = new(StringComparer.Ordinal);

    /// <summary>
    /// How many distinct identities the process shares providers for. Past this, a caller gets a provider
    /// of its own instead of growing a dictionary that is never emptied — correct, only without the
    /// sharing. It bounds a process that opens connections under an unbounded number of user names; no
    /// ordinary deployment approaches it.
    /// </summary>
    private const int MaxSharedProviders = 1024;

    /// <summary>
    /// Returns the process-wide provider for a set of credentials, creating it on first use. Sharing
    /// matters most for Entity Framework, which builds a fresh
    /// <see cref="CamusConnectionStringBuilder"/> for every <see cref="System.Data.Common.DbConnection"/>
    /// it opens: without this, each connection would perform its own login, and a per-account limit of 20
    /// logins a minute would start rejecting them. Keyed by credentials rather than by connection string
    /// so connections differing only in database or timeout still share one token.
    /// </summary>
    public static CamusTokenProvider Shared(string key, Func<CamusTokenProvider> factory)
    {
        if (SharedProviders.TryGetValue(key, out CamusTokenProvider? existing))
            return existing;

        if (SharedProviders.Count >= MaxSharedProviders)
            return factory();

        return SharedProviders.GetOrAdd(key, _ => factory());
    }

    /// <summary>
    /// A per-process random salt for <see cref="SharingKey"/>. The key lives in a dictionary for the life
    /// of the process, and an unsalted hash of a password is an offline verifier for anyone who reads
    /// process memory. The key only has to be stable within one process, so a salt costs nothing.
    /// </summary>
    private static readonly byte[] SharingKeySalt = RandomNumberGenerator.GetBytes(32);

    /// <summary>
    /// A stable, non-reversible identity for a credential set + login endpoint. Hashed, and salted per
    /// process, so a long-lived dictionary key neither holds a plaintext password nor can be tested
    /// against a guessed one outside this process.
    /// </summary>
    public static string SharingKey(CamusCredentials credentials, string authEndpointConfig)
    {
        string material = string.Join(
            '\n', credentials.User ?? "", credentials.Password ?? "", credentials.AccessToken ?? "", authEndpointConfig);

        byte[] bytes = Encoding.UTF8.GetBytes(material);

        byte[] hash = new byte[SHA256.HashSizeInBytes];
        HMACSHA256.HashData(SharingKeySalt, bytes, hash);

        CryptographicOperations.ZeroMemory(bytes);

        return Convert.ToHexString(hash);
    }

    // Resolved on first use, not at construction: for gRPC the login client is the transport itself, and
    // the transport is built with this provider — so eager resolution would be a cycle.
    private readonly Func<ICamusLoginClient> resolveLoginClient;
    private readonly Func<string> resolveEndpoint;
    private readonly Func<int> resolveTimeoutSeconds;
    private readonly TimeSpan lifetime;
    private readonly TimeProvider clock;

    // Serializes logins so N concurrent statements on a cold/stale token produce one /login, not N.
    private readonly SemaphoreSlim gate = new(1, 1);

    /// <summary>Credentials + cached token + its two deadlines as one immutable snapshot, so the
    /// per-request read (<see cref="TryGetCached"/> runs on every statement) is a lock-free volatile
    /// load. Writers replace the whole object under <see cref="stateLock"/>. From <see cref="RenewAfter"/>
    /// the token is still sent, and a background login replaces it; from <see cref="ExpiresAt"/> it is not
    /// sent any more, and a statement waits for the login.</summary>
    private sealed record AuthState(CamusCredentials Credentials, string? Token, DateTimeOffset RenewAfter, DateTimeOffset ExpiresAt);

    private readonly object stateLock = new();
    private AuthState state = null!;   // assigned by SetCredentials in the constructor

    /// <summary>The share of the server-reported lifetime the driver sends a token for. The lifetime starts
    /// when the server issues the token, and the driver starts its count before it sends the login, so the
    /// count is early. The remaining 5% is for the request that carries the token to reach the server.</summary>
    private const double UsableShareOfReportedLifetime = 0.95;

    /// <summary>How long the provider waits before it starts another background renewal after one failed.
    /// The token is still valid, so the wait costs nothing. Without the wait, every statement after a failed
    /// renewal would start a new login, against a server that is down or rate-limits this account.</summary>
    internal static readonly TimeSpan BackgroundRenewalRetryDelay = TimeSpan.FromSeconds(5);

    // 1 while a background renewal is queued or running, so at most one is in flight per provider.
    private int renewing;

    // UTC ticks before which no background renewal starts, after a failed one.
    private long renewalRetryAfterTicks;

    public CamusTokenProvider(
        CamusCredentials credentials,
        Func<ICamusLoginClient> resolveLoginClient,
        Func<string> resolveEndpoint,
        Func<int> resolveTimeoutSeconds,
        TimeSpan? lifetime = null,
        TimeProvider? clock = null)
    {
        this.resolveLoginClient = resolveLoginClient;
        this.resolveEndpoint = resolveEndpoint;
        this.resolveTimeoutSeconds = resolveTimeoutSeconds;
        this.lifetime = lifetime ?? DefaultLifetime;
        this.clock = clock ?? TimeProvider.System;

        SetCredentials(credentials);
    }

    /// <summary>
    /// True when this connection has anything to authenticate with. When false the driver sends no
    /// <c>Authorization</c> header, which is what an unauthenticated server (the default) expects.
    /// </summary>
    public bool IsEnabled => Volatile.Read(ref state).Credentials.IsSet;

    /// <summary>True when the provider holds a password and can therefore replace a rejected token.</summary>
    public bool CanRenew => Volatile.Read(ref state).Credentials.CanRenew;

    /// <summary>
    /// The cached token without triggering a login, for callers that cannot await — specifically the gRPC
    /// batch-stream factory, which the batcher invokes synchronously when it (re)builds a stream. Every
    /// such call site awaits <see cref="GetTokenAsync"/> first, so the cache is warm by then.
    /// </summary>
    public string? CurrentToken => Volatile.Read(ref state).Token;

    /// <summary>True while a background renewal is queued or running, including its failure handling.</summary>
    internal bool RenewalInFlight => Volatile.Read(ref renewing) == 1;

    /// <summary>
    /// Returns a usable bearer token. A token past its renewal point but not yet expired is returned at
    /// once, and one background login replaces it, so a statement does not wait for a token it does not
    /// need yet. The caller waits for a login only when there is no token, or the token expired.
    /// Returns null when no credentials are configured.
    /// </summary>
    public async ValueTask<string?> GetTokenAsync(CancellationToken cancellationToken)
    {
        if (TryGetCached(out string? cached, out bool enabled) || !enabled)
            return cached;

        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // A caller that queued behind the winner finds the token it minted.
            if (TryGetCached(out cached, out enabled) || !enabled)
                return cached;

            CamusCredentials current = Volatile.Read(ref state).Credentials;

            if (!current.CanRenew)
            {
                // A directly supplied token that has been invalidated: nothing left to authenticate with.
                throw new CamusException(
                    CamusAuthErrorCodes.AuthenticationFailed,
                    "The supplied access token was rejected and the connection has no credentials to obtain a new one");
            }

            try
            {
                string token = await LoginLockedAsync(current.User!, current.Password!, cancellationToken).ConfigureAwait(false);
                CamusMetrics.RecordLogin(CamusMetrics.LoginMode.Blocking, errorType: null);
                return token;
            }
            catch (Exception ex)
            {
                CamusMetrics.RecordLogin(CamusMetrics.LoginMode.Blocking, (ex as CamusException)?.Code ?? ex.GetType().FullName);
                throw;
            }
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Starts one login in the background to replace <paramref name="stale"/>, a token past its renewal
    /// point that callers still use. At most one renewal is in flight per provider, and after a failed one
    /// the next starts only after <see cref="BackgroundRenewalRetryDelay"/>.
    /// </summary>
    private void StartBackgroundRenewal(AuthState stale)
    {
        if (clock.GetUtcNow().UtcTicks < Interlocked.Read(ref renewalRetryAfterTicks))
            return;

        if (Interlocked.CompareExchange(ref renewing, 1, 0) != 0)
            return;

        // The renewal belongs to no statement. Without this, it would carry the trace of the statement that
        // started it, and its HTTP span would show as part of a request that did not wait for it.
        using (ExecutionContext.SuppressFlow())
            _ = Task.Run(() => RenewInBackgroundAsync(stale));
    }

    private async Task RenewInBackgroundAsync(AuthState stale)
    {
        try
        {
            await gate.WaitAsync().ConfigureAwait(false);
            try
            {
                // Anything that replaced the state while this renewal waited for the gate (a blocking login, an
                // explicit login or logout, an invalidation) already decided what the next token is.
                if (!ReferenceEquals(Volatile.Read(ref state), stale))
                    return;

                await LoginLockedAsync(stale.Credentials.User!, stale.Credentials.Password!, CancellationToken.None).ConfigureAwait(false);
                CamusMetrics.RecordLogin(CamusMetrics.LoginMode.Background, errorType: null);
            }
            finally
            {
                gate.Release();
            }
        }
        catch (Exception ex)
        {
            // The old token is still valid, so nothing has failed yet: statements keep sending it, and a later
            // statement tries again after the delay. If the token expires first, the next statement logs in
            // itself and surfaces the error.
            Interlocked.Exchange(ref renewalRetryAfterTicks, (clock.GetUtcNow() + BackgroundRenewalRetryDelay).UtcTicks);
            CamusMetrics.RecordLogin(CamusMetrics.LoginMode.Background, (ex as CamusException)?.Code ?? ex.GetType().FullName);
        }
        finally
        {
            Volatile.Write(ref renewing, 0);
        }
    }

    /// <summary>
    /// Discards <paramref name="staleToken"/> so the next <see cref="GetTokenAsync"/> mints a new one.
    /// Comparing against the cached value keeps a late rejection of an already-replaced token from
    /// throwing away the good one a concurrent caller just obtained. A no-op when the token cannot be
    /// renewed (<c>AccessToken=</c>), so the caller surfaces the failure instead of looping.
    /// </summary>
    public void Invalidate(string? staleToken)
    {
        if (string.IsNullOrEmpty(staleToken))
            return;

        lock (stateLock)
        {
            AuthState current = state;
            if (current.Credentials.CanRenew && string.Equals(current.Token, staleToken, StringComparison.Ordinal))
                Volatile.Write(ref state, current with { Token = null });
        }
    }

    /// <summary>
    /// Authenticates explicitly, replacing whatever credentials the connection string supplied, and
    /// returns the minted token. Any previously cached token is dropped.
    /// </summary>
    public async Task<string> LoginAsync(string user, string password, CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (stateLock)
                Volatile.Write(ref state, state with { Credentials = CamusCredentials.FromPassword(user, password), Token = null });

            try
            {
                string token = await LoginLockedAsync(user, password, cancellationToken).ConfigureAwait(false);
                CamusMetrics.RecordLogin(CamusMetrics.LoginMode.Explicit, errorType: null);
                return token;
            }
            catch (Exception ex)
            {
                CamusMetrics.RecordLogin(CamusMetrics.LoginMode.Explicit, (ex as CamusException)?.Code ?? ex.GetType().FullName);
                throw;
            }
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Revokes the cached token server-side and forgets it. The configured credentials are kept, so a
    /// later statement transparently authenticates again; call this to end a session, not to switch it
    /// off. A no-op when no token has been minted yet.
    /// </summary>
    public async Task LogoutAsync(CancellationToken cancellationToken)
    {
        await gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            string? revoked;
            lock (stateLock)
            {
                revoked = state.Token;
                Volatile.Write(ref state, state with { Token = null });
            }

            if (string.IsNullOrEmpty(revoked))
                return;

            await resolveLoginClient().LogoutAsync(resolveEndpoint(), revoked, resolveTimeoutSeconds(), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    // Caller holds the gate.
    private async Task<string> LoginLockedAsync(string user, string password, CancellationToken cancellationToken)
    {
        // Both deadlines count from before the request, so the time the login takes is charged to the token.
        DateTimeOffset requestedAt = clock.GetUtcNow();

        CamusLoginResult minted = await resolveLoginClient()
            .LoginAsync(resolveEndpoint(), user, password, resolveTimeoutSeconds(), cancellationToken)
            .ConfigureAwait(false);

        (TimeSpan renewAfter, TimeSpan expiresAfter) = Deadlines(minted.ExpiresIn);

        lock (stateLock)
            Volatile.Write(ref state, state with { Token = minted.Token, RenewAfter = requestedAt + renewAfter, ExpiresAt = requestedAt + expiresAfter });

        return minted.Token;
    }

    /// <summary>
    /// When to start the next login, and when to stop sending the token. The server's reported lifetime is
    /// authoritative (its TTL is configurable and may be shorter than any value the driver defaults to):
    /// renew at 80% of it, and send the token until 95% of it. Absent a reported value (an older server),
    /// the configured <c>TokenLifetime</c> is the lifetime the driver assumes: renew at 80% of it, and do
    /// not send the token past it.
    /// </summary>
    private (TimeSpan RenewAfter, TimeSpan ExpiresAfter) Deadlines(TimeSpan? reported)
    {
        TimeSpan expiresAfter = reported is { } lifetimeFromServer
            ? lifetimeFromServer * UsableShareOfReportedLifetime
            : lifetime;

        TimeSpan renewAfter = (reported ?? lifetime) * 0.8;

        if (expiresAfter <= TimeSpan.Zero)
            expiresAfter = TimeSpan.FromSeconds(1);

        if (renewAfter <= TimeSpan.Zero || renewAfter > expiresAfter)
            renewAfter = expiresAfter;

        return (renewAfter, expiresAfter);
    }

    private void SetCredentials(CamusCredentials value)
    {
        lock (stateLock)
        {
            // A directly supplied token is the cached token, and never ages out on our side.
            Volatile.Write(ref state, new AuthState(value, value.AccessToken, DateTimeOffset.MaxValue, DateTimeOffset.MaxValue));
        }
    }

    /// <summary>Reports the cached token when it can still be sent; <paramref name="enabled"/> tells a false
    /// return apart from "there is nothing to authenticate with". A token past its renewal point is still
    /// reported, and starts the background renewal. Lock-free on the common path: one volatile snapshot,
    /// taken on every statement.</summary>
    private bool TryGetCached(out string? cached, out bool enabled)
    {
        AuthState snapshot = Volatile.Read(ref state);

        enabled = snapshot.Credentials.IsSet;

        if (enabled && snapshot.Token is not null)
        {
            DateTimeOffset now = clock.GetUtcNow();

            if (now < snapshot.ExpiresAt)
            {
                if (now >= snapshot.RenewAfter && snapshot.Credentials.CanRenew)
                    StartBackgroundRenewal(snapshot);

                cached = snapshot.Token;
                return true;
            }
        }

        cached = null;
        return false;
    }
}
