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
/// <para><b>Renewal.</b> The <c>/login</c> reply reports how long the token is good for, and the provider
/// renews at 80% of that. Against a server predating the field it falls back to its own conservative
/// lifetime (<c>TokenLifetime=</c>, default 10 minutes against the server's 15-minute default
/// <c>AccessTokenTtl</c>). That proactive renewal is the primary path;
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
    /// Returns the process-wide provider for a set of credentials, creating it on first use. Sharing
    /// matters most for Entity Framework, which builds a fresh
    /// <see cref="CamusConnectionStringBuilder"/> for every <see cref="System.Data.Common.DbConnection"/>
    /// it opens: without this, each connection would perform its own login, and a per-account limit of 20
    /// logins a minute would start rejecting them. Keyed by credentials rather than by connection string
    /// so connections differing only in database or timeout still share one token.
    /// </summary>
    public static CamusTokenProvider Shared(string key, Func<CamusTokenProvider> factory)
        => SharedProviders.GetOrAdd(key, _ => factory());

    /// <summary>
    /// A stable, non-reversible identity for a credential set + login endpoint. Hashed so a long-lived
    /// dictionary key never holds a plaintext password.
    /// </summary>
    public static string SharingKey(CamusCredentials credentials, string authEndpointConfig)
    {
        string material = string.Join(
            '\n', credentials.User ?? "", credentials.Password ?? "", credentials.AccessToken ?? "", authEndpointConfig);

        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(material)));
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

    /// <summary>Credentials + cached token + renewal deadline as one immutable snapshot, so the
    /// per-request read (<see cref="TryGetCached"/> runs on every statement) is a lock-free volatile
    /// load. Writers replace the whole object under <see cref="stateLock"/>.</summary>
    private sealed record AuthState(CamusCredentials Credentials, string? Token, DateTimeOffset RenewAfter);

    private readonly object stateLock = new();
    private AuthState state = null!;   // assigned by SetCredentials in the constructor

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

    /// <summary>
    /// Returns a usable bearer token, logging in if there is none or the cached one has aged out.
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

            return await LoginLockedAsync(current.User!, current.Password!, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
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

            return await LoginLockedAsync(user, password, cancellationToken).ConfigureAwait(false);
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
        CamusLoginResult minted = await resolveLoginClient()
            .LoginAsync(resolveEndpoint(), user, password, resolveTimeoutSeconds(), cancellationToken)
            .ConfigureAwait(false);

        lock (stateLock)
            Volatile.Write(ref state, state with { Token = minted.Token, RenewAfter = clock.GetUtcNow() + RenewalDelay(minted.ExpiresIn) });

        return minted.Token;
    }

    /// <summary>
    /// When to mint the next token. The server's reported lifetime is authoritative — its TTL is
    /// configurable and may be shorter than any value the driver defaults to — so renew at 80% of it,
    /// leaving headroom for the request that carries the token to complete. Absent a reported value (an
    /// older server) fall back to the configured <c>TokenLifetime</c>, which is already conservative
    /// against the 15-minute default TTL.
    /// </summary>
    private TimeSpan RenewalDelay(TimeSpan? reported)
    {
        if (reported is not { } lifetimeFromServer)
            return lifetime;

        TimeSpan renewal = lifetimeFromServer * 0.8;

        return renewal > TimeSpan.Zero ? renewal : TimeSpan.FromSeconds(1);
    }

    private void SetCredentials(CamusCredentials value)
    {
        lock (stateLock)
        {
            // A directly supplied token is the cached token, and never ages out on our side.
            Volatile.Write(ref state, new AuthState(value, value.AccessToken, DateTimeOffset.MaxValue));
        }
    }

    /// <summary>Reports the cached token when it is still within its lifetime; <paramref name="enabled"/>
    /// tells a false return apart from "there is nothing to authenticate with". Lock-free: one volatile
    /// snapshot, taken on every statement.</summary>
    private bool TryGetCached(out string? cached, out bool enabled)
    {
        AuthState snapshot = Volatile.Read(ref state);

        enabled = snapshot.Credentials.IsSet;

        if (enabled && snapshot.Token is not null && clock.GetUtcNow() < snapshot.RenewAfter)
        {
            cached = snapshot.Token;
            return true;
        }

        cached = null;
        return false;
    }
}
