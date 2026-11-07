# Transactions

Explicit transactions in `CamusDB.Client`: pipelining, isolation, locking, and retries.

## Transactions

```csharp
CamusTransaction transaction = await connection.BeginTransactionAsync();

await using CamusCommand insert = connection.CreateInsertCommand("robots");
insert.Transaction = transaction;

insert.Parameters.Add("id", ColumnType.Id, CamusObjectIdGenerator.Generate());
insert.Parameters.Add("name", ColumnType.String, "HAL 9000");
insert.Parameters.Add("type", ColumnType.String, "electronic");
insert.Parameters.Add("year", ColumnType.Integer64, 1968);
insert.Parameters.Add("price", ColumnType.Float64, 42.0);
insert.Parameters.Add("enabled", ColumnType.Bool, true);

await insert.ExecuteNonQueryAsync();
await transaction.CommitAsync();
```

`RollbackAsync()` rolls back. Disposing an unfinished transaction rolls it back, so `await using` never leaves its locks open.

## Pipelined transactions

A pipeline (`tx.CreatePipeline()`) sends several statements of one transaction in one round trip. `QueueReader`/`QueueNonQuery` return each statement's task. `SendAsync` sends the queue. `CommitAsync` sends it plus a commit that runs only if every statement succeeded; do not call `tx.CommitAsync` after it. With `DeferBegin = true` or [learned routing](learned-routing.md), `BEGIN` rides the first send.

```csharp
CamusTransaction tx = await connection.BeginTransactionAsync(new CamusTransactionOptions
{
    Locking = CamusLocking.Optimistic,
    DeferBegin = true,                              // BEGIN travels inside the first send
});
CamusPipeline pipeline = tx.CreatePipeline();

// Exchange 1: [BEGIN, SELECT a, SELECT b]
CamusCommand readA = connection.CreateSelectCommand("SELECT balance FROM accounts WHERE id = @id");
readA.Parameters.Add("@id", ColumnType.Id, a);
CamusCommand readB = connection.CreateSelectCommand("SELECT balance FROM accounts WHERE id = @id");
readB.Parameters.Add("@id", ColumnType.Id, b);
Task<CamusDataReader> balanceA = pipeline.QueueReader(readA);
Task<CamusDataReader> balanceB = pipeline.QueueReader(readB);
await pipeline.SendAsync();

long fromBalance = await ReadBalance(await balanceA);
long toBalance = await ReadBalance(await balanceB);

// Exchange 2: [UPDATE a, UPDATE b, COMMIT] — the commit happens only if both updates succeeded.
CamusCommand debit = connection.CreateCamusCommand("UPDATE accounts SET balance = @v WHERE id = @id");
debit.Parameters.Add("@v", ColumnType.Integer64, fromBalance - amount);
debit.Parameters.Add("@id", ColumnType.Id, a);
CamusCommand credit = connection.CreateCamusCommand("UPDATE accounts SET balance = @v WHERE id = @id");
credit.Parameters.Add("@v", ColumnType.Integer64, toBalance + amount);
credit.Parameters.Add("@id", ColumnType.Id, b);
Task<int> debited = pipeline.QueueNonQuery(debit);
Task<int> credited = pipeline.QueueNonQuery(credit);
await pipeline.CommitAsync();

Console.WriteLine($"{pipeline.Exchanges} round trips");   // 2 over gRPC
```

- Each task gets its own result or `CamusException`; `SendAsync`/`CommitAsync` also throw the first failure.
- A failure ends the transaction: later statements fail and nothing commits. The server has rolled back (`RollbackAsync` is a no-op). Replay from `BEGIN`.
- Statements run in queue order. You may send again, or run other commands between sends, but one send at a time.
- `Exchanges` counts round trips: one per send over gRPC (server frame contract 2+). REST, `RequestFrames=false` or an older server cost one per statement, same results.
- DDL, `TRUNCATE`, `CamusInsertCommand` and commands of another transaction cannot be queued; they throw, and the pipeline stays usable.

## Isolation level, mode, and optimistic locking

Options in `CamusTransactionOptions`:

| Option | Values | Default |
| --- | --- | --- |
| `IsolationLevel` | `Serializable`, `ReadCommitted` | server default (Serializable) |
| `Mode` | `ReadWrite`, `ReadOnly` | `ReadWrite` |
| `Locking` | `Pessimistic`, `Optimistic` | server default (Pessimistic) |

Pessimistic locking locks rows as it reads and writes. Optimistic locking takes no write locks and checks conflicts at commit; the loser throws a `CamusException` to [replay](#serializable-isolation--retries). Use it for low-contention, read-mostly work.

Only `Serializable` prevents phantoms (rows another transaction inserts into a range you read). Read Committed + Optimistic takes no locks and allows them. Serializable + Optimistic still takes shared range locks.

```csharp
// Optimistic transaction
CamusTransaction tx = await connection.BeginTransactionAsync(CamusTransactionOptions.Optimistic);

// Or select knobs explicitly
CamusTransaction rc = await connection.BeginTransactionAsync(new CamusTransactionOptions
{
    IsolationLevel = CamusIsolationLevel.ReadCommitted,
    Locking = CamusLocking.Optimistic,
});

// A lock-free consistent snapshot (Serializable read-only), resumable across requests
CamusTransaction snap = await connection.BeginTransactionAsync(CamusTransactionOptions.Snapshot);
```

The keys `IsolationLevel=`, `TransactionMode=`, `Locking=` (case-insensitive) set defaults, also for autocommit statements:

```
Endpoint=http://localhost:5095;Database=test;Locking=Optimistic;IsolationLevel=ReadCommitted;TransactionMode=ReadWrite
```

Precedence: transaction options › connection string › server. `SET TRANSACTION LOCKING { PESSIMISTIC | OPTIMISTIC }` or `SET TRANSACTION ISOLATION LEVEL …` as a transaction's first statement also works.

## Serializable Isolation & Retries

On a conflict the server aborts one transaction. Replay it from `BEGIN`, never only the failed statement, on these codes:

| Code | Name | When raised |
| --- | --- | --- |
| `CADB0502` | `TransactionConflict` | Lock conflict; server aborted at lock-acquire time |
| `CADB0504` | `TransactionMustRetry` | Routing retry budget exhausted; no data written |
| `CADB0505` | `TransactionLifetimeExceeded` | Transaction held open past the server lifetime limit |

Never replay on `CADB0509` (`TransactionFinalizeUnresolved`): the commit may already be durable. `CommitAsync`/`RollbackAsync` re-issue the finalize themselves.

`SerializableRetryHelper.IsRetryable(ex)` is `true` for the three codes and the messages `MustRetry`, `AlreadyLocked`, `commit returned Aborted`. `ExecuteAutocommitAsync` reruns the whole delegate:

```csharp
await SerializableRetryHelper.ExecuteAutocommitAsync(async ct =>
{
    CamusTransaction tx = await connection.BeginTransactionAsync(ct);
    try
    {
        await using CamusCommand cmd = connection.CreateCamusCommand(
            "UPDATE robots SET price = @price WHERE name = @name");
        cmd.Transaction = tx;
        cmd.Parameters.Add("@price", ColumnType.Float64, 99.0);
        cmd.Parameters.Add("@name",  ColumnType.String,  "T-800");
        await cmd.ExecuteNonQueryAsync(ct);
        await tx.CommitAsync(ct);
    }
    catch
    {
        await tx.RollbackAsync(ct);
        throw;
    }
}, maxAttempts: 5, cancellationToken);
```

Back-off: `min(20 ms × 2^attempt, 400 ms)` ± 25 % jitter. Other exceptions propagate. Or write the loop yourself:

```csharp
const int MaxAttempts = 5;
int attempt = 0;

while (true)
{
    CamusTransaction tx = await connection.BeginTransactionAsync();
    try
    {
        // re-execute ALL reads and writes on every attempt
        long balance = await ReadBalance(tx, accountId);
        if (balance < amount)
            throw new InvalidOperationException("Insufficient funds");
        await Debit(tx, accountId, balance - amount);
        await tx.CommitAsync();
        break;
    }
    catch (CamusException ex) when (SerializableRetryHelper.IsRetryable(ex))
    {
        await tx.RollbackAsync();
        if (++attempt >= MaxAttempts)
            throw;
        await Task.Delay(20 * (1 << attempt));
    }
    catch
    {
        await tx.RollbackAsync();
        throw;
    }
}
```
