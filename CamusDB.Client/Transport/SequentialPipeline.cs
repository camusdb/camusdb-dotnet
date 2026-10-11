/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client.Auth;

namespace CamusDB.Client.Transport;

/// <summary>
/// Runs a <see cref="TransportPipelineRequest"/> one exchange per step over any transport's ordinary
/// calls: <c>BEGIN</c>, each statement in order, then the commit. It is the fallback the pipeline API
/// promises when the transport cannot send the request as one exchange — REST always, gRPC against a
/// server that announced no pipeline contract — so a caller gets the same outcome shape either way and
/// only the exchange count differs.
///
/// <para>A failed statement ends the transaction on the server (the server rolls it back), so the
/// statements after it are not sent; they are reported with that same failure, which is what the server
/// would have answered them with, in substance, had they been sent. The commit is then not attempted
/// and reports the failure too.</para>
/// </summary>
internal static class SequentialPipeline
{
    public static async Task<PipelineTransportResult> RunAsync(
        ICamusTransport transport, TransportPipelineRequest request, CancellationToken cancellationToken)
    {
        int exchanges = 0;
        StartTransactionResult? started = null;
        long? txnIdPT = request.TxnIdPT;
        uint? txnIdCounter = request.TxnIdCounter;
        int? streamSlot = request.StreamSlot;

        if (request.Start is { } options)
        {
            try
            {
                started = await transport.StartTransactionAsync(
                    request.Endpoint, request.Database, options, request.TimeoutSeconds, cancellationToken).ConfigureAwait(false);
                exchanges++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // A rejected credential is thrown rather than reported, so the authenticating wrapper
                // can renew it and replay: nothing ran, so the replay is safe.
                if (ex is CamusException { Code: CamusAuthErrorCodes.AuthenticationFailed })
                    throw;

                return new PipelineTransportResult
                {
                    StartFailure = ex,
                    Statements = AllFailed(request.Statements.Count, ex),
                    CommitFailure = request.Commit ? ex : null,
                    Exchanges = exchanges,
                };
            }

            txnIdPT = started.Value.TxnIdPT;
            txnIdCounter = started.Value.TxnIdCounter;
            streamSlot = started.Value.StreamSlot;
        }

        PipelineStatementOutcome[] outcomes = new PipelineStatementOutcome[request.Statements.Count];
        Exception? first = null;

        for (int i = 0; i < outcomes.Length; i++)
        {
            if (first is not null)
            {
                outcomes[i] = PipelineStatementOutcome.Failed(first);
                continue;
            }

            TransportPipelineStatement statement = request.Statements[i];
            TransportSqlRequest sql = new()
            {
                Endpoint = request.Endpoint,
                Database = request.Database,
                Sql = statement.Sql,
                Parameters = statement.Parameters,
                TxnIdPT = txnIdPT,
                TxnIdCounter = txnIdCounter,
                StreamSlot = streamSlot,
                TimeoutSeconds = request.TimeoutSeconds,
                Prepared = statement.Prepared,
                DiscardReturningRows = statement.DiscardReturningRows,
            };

            try
            {
                outcomes[i] = statement.Kind == PipelineStatementKind.Query
                    ? new PipelineStatementOutcome { Query = await transport.ExecuteQueryAsync(sql, cancellationToken).ConfigureAwait(false) }
                    : new PipelineStatementOutcome { NonQuery = await transport.ExecuteNonQueryAsync(sql, cancellationToken).ConfigureAwait(false) };
                exchanges++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                exchanges++;
                first = ex;
                outcomes[i] = PipelineStatementOutcome.Failed(ex);
            }
        }

        if (!request.Commit)
            return new PipelineTransportResult { Started = started, Statements = outcomes, Exchanges = exchanges };

        if (first is not null)
            return new PipelineTransportResult { Started = started, Statements = outcomes, CommitFailure = first, Exchanges = exchanges };

        try
        {
            await transport.FinalizeTransactionAsync(
                commit: true, request.Endpoint, request.Database, txnIdPT!.Value, txnIdCounter!.Value, streamSlot,
                request.TimeoutSeconds, cancellationToken).ConfigureAwait(false);
            exchanges++;
            return new PipelineTransportResult { Started = started, Statements = outcomes, Committed = true, Exchanges = exchanges };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            exchanges++;
            return new PipelineTransportResult { Started = started, Statements = outcomes, CommitFailure = ex, Exchanges = exchanges };
        }
    }

    private static PipelineStatementOutcome[] AllFailed(int count, Exception failure)
    {
        PipelineStatementOutcome[] outcomes = new PipelineStatementOutcome[count];
        for (int i = 0; i < count; i++)
            outcomes[i] = PipelineStatementOutcome.Failed(failure);
        return outcomes;
    }
}
