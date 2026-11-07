# Authentication

How to connect to a server that requires a user and password, how the driver manages tokens, and the TLS rules.

## Connecting with credentials

Authentication is off by default; without credentials the driver sends no `Authorization` header. For a server started with `CAMUSDB_AUTH_ENABLED=true` (see the server's `docs/sql-authentication.md`), add `User` and `Password`:

```csharp
CamusConnectionStringBuilder builder = new(
    "Endpoint=http://localhost:5095;Database=test;User=app;Password=app-secret");

await using CamusConnection connection = new(builder);
await connection.OpenAsync();

// Nothing else changes — the first statement authenticates transparently.
await using CamusCommand command = connection.CreateCamusCommand("SELECT * FROM orders");
```

The driver logs in once (`POST /login` over REST, the `CamusAuth` service over gRPC) and gets a short-lived bearer token (a string that proves identity). Every statement then sends the token, never the password.

## Token handling

- **One login per credential set.** The token is cached per process, credentials and server, so a connection pool or EF Core logs in once. Logins are slow on purpose (PBKDF2, 600,000 iterations), and the server allows 20 per account per minute.
- **Background renewal.** The server reports the token lifetime (`AccessTokenTtl`). At 80% of it the driver logs in again in the background; statements keep the current token and do not wait. A statement waits only when there is no token, or the token reached 95% of its lifetime without renewal. A failed renewal is retried after 5 seconds.
- **No reported expiry.** The driver uses `TokenLifetime` (default 600 seconds; server default 15 minutes), renews at 80%, and never sends the token past it.
- **Idle apps.** After an idle period longer than the lifetime, the next statement waits for a login.
- **Rejected token.** After a password change, `DROP USER`, a logout elsewhere or a server restart, the driver logs in again and replays the statement once. This is safe: a rejected request never ran.
- **`CADB0517` (insufficient privilege) is never retried.** Grant the privilege with `GRANT … ON db.* TO user`.

## Authenticating explicitly

To keep a password from a secret manager out of the connection string, log in on the open connection:

```csharp
await using CamusConnection connection = new(new CamusConnectionStringBuilder("Endpoint=http://localhost:5095;Database=test"));
await connection.OpenAsync();

string token = await connection.LoginAsync("app", await secrets.GetAsync("camus-password"));

// … statements …

await connection.LogoutAsync();   // revokes the token server-side
```

- `connection.AccessToken` returns the cached token, or `null`. Pass it to another process with `AccessToken=`.
- An `AccessToken=` token is sent as is and never renewed. If the server rejects it, the driver throws `CamusException` with `CADB0516` and does not retry.
- `LogoutAsync` revokes the token but keeps configured credentials; the next statement logs in again.

## Authenticating over gRPC

Credentials work as over REST:

```csharp
CamusConnectionStringBuilder builder = new(
    "Endpoint=http://localhost:5096;Database=test;Protocol=grpc;User=app;Password=app-secret");
```

The login uses the `CamusAuth` service on the statement channel, so a gRPC-only deployment needs no HTTP port. The token travels in the `authorization` metadata.

The server reads the token once per `BatchExecute` stream (see [Transport](transport.md)). After a renewal, the driver replaces each stream on the next statement:

- Open transactions stay on the old stream, which takes no new work and runs their statements unprepared.
- The old stream closes after its last transaction ends, or after 5 minutes, so an abandoned transaction cannot hold its locks.
- No statement fails because of the switch. `AccessToken=` streams are never replaced.

## TLS

With authentication on, the server refuses credentials over plain HTTP (`RequireTlsWhenAuthEnabled`, on by default), except on loopback. Use `https://` for any other host, or the server answers `CADB0519`.

If a proxy terminates TLS in front of the node (ingress, sidecar, service mesh), start the server with `--require-tls-when-auth-enabled false` or set `require_tls_when_auth_enabled: false` in `config.yml`. Keep the plain hop inside your trusted network.

### The client refuses first

If the connection string has credentials (`User=` or `AccessToken=`) and an endpoint that is neither `https://` nor loopback, the builder constructor throws `CADB0519`. This check runs before the password leaves the process.

`BackupEndpoint` follows the same rule. It receives the connection's superuser-grade token, so point it at the same deployment as `Endpoint`.

For the proxy case, set `AllowInsecureCredentials=true`:

```csharp
CamusConnectionStringBuilder builder = new(
    "Endpoint=http://camus.internal:5095;Database=test;User=app;Password=app-secret;AllowInsecureCredentials=true");
```

## Error codes

| Code | HTTP | Meaning |
| --- | --- | --- |
| `CADB0516` | 401 | Authentication failed: bad or expired token, unknown user, or wrong password. One code for all, so replies do not reveal accounts. |
| `CADB0517` | 403 | The user lacks a privilege the statement needs on a table. |
| `CADB0518` | 429 | Too many login attempts for the account. |
| `CADB0519` | 400 | Credentials over plain HTTP where TLS is required. |

## Managing users

Run `CREATE USER`, `GRANT`, `REVOKE` and `SHOW GRANTS` as a superuser. Bind the password as a parameter, so it stays out of logs and traces:

```csharp
await using CamusCommand command = connection.CreateCamusCommand("CREATE USER app IDENTIFIED BY @password");
command.Parameters.Add("@password", ColumnType.String, "app-secret");
await command.ExecuteNonQueryAsync();
```
