using System.Data;
using Dapper;
using Softela.PestCalendarDataProcessor.Data;
using Softela.PestCalendarDataProcessor.Models;

namespace Softela.PestCalendarDataProcessor.Repositories;

public sealed class OutboxRepository(IDapperDataContext dapperDataContext) : IOutboxRepository
{
    private readonly IDapperDataContext _dapperDataContext = dapperDataContext;

    public async Task<List<OutboxMessage>> GetUnprocessedAsync(int batchSize, DateTimeOffset now, TimeSpan stalenessWindow, CancellationToken cancellationToken = default)
    {
        var staleThreshold = now - stalenessWindow;
        var claimToken = Guid.NewGuid();

        var parameters = new DynamicParameters();
        parameters.Add("batch_size", batchSize, DbType.Int32);
        parameters.Add("claimed_at", now, DbType.DateTimeOffset);
        parameters.Add("claim_token", claimToken, DbType.Guid);
        parameters.Add("stale_threshold", staleThreshold, DbType.DateTimeOffset);

        const string sql = """
            WITH batch AS (
                SELECT id FROM outboxmessages
                WHERE processedat IS NULL
                  AND error IS NULL
                  AND (claimedat IS NULL OR claimedat < :stale_threshold)
                ORDER BY occurredat
                LIMIT :batch_size
                FOR UPDATE SKIP LOCKED
            ),
            updated AS (
                UPDATE outboxmessages
                SET claimedat = :claimed_at, claimtoken = :claim_token
                FROM batch
                WHERE outboxmessages.id = batch.id
                RETURNING outboxmessages.id, eventtype, payload, occurredat, claimedat, claimtoken, processedat, error
            )
            SELECT * FROM updated ORDER BY occurredat
            """;

        var command = new CommandDefinition(
            commandText: sql,
            parameters: parameters,
            transaction: _dapperDataContext.Transaction,
            commandType: CommandType.Text,
            cancellationToken: cancellationToken
        );

        var messages = await _dapperDataContext.Connection.QueryAsync<OutboxMessage>(command).ConfigureAwait(false);
        return messages.ToList();
    }

    public async Task MarkAsProcessedAsync(long id, Guid claimToken, DateTimeOffset processedAt, CancellationToken cancellationToken = default)
    {
        var parameters = new DynamicParameters();
        parameters.Add("p_id", id, DbType.Int64);
        parameters.Add("p_claim_token", claimToken, DbType.Guid);
        parameters.Add("p_processed_at", processedAt, DbType.DateTimeOffset);

        var command = new CommandDefinition(
            commandText: "UPDATE outboxmessages SET processedat = :p_processed_at WHERE id = :p_id AND claimtoken = :p_claim_token",
            parameters: parameters,
            transaction: _dapperDataContext.Transaction,
            commandType: CommandType.Text,
            cancellationToken: cancellationToken
        );

        await _dapperDataContext.Connection.ExecuteAsync(command).ConfigureAwait(false);
    }

    public async Task MarkAsFailedAsync(long id, Guid claimToken, string error, CancellationToken cancellationToken = default)
    {
        var parameters = new DynamicParameters();
        parameters.Add("p_id", id, DbType.Int64);
        parameters.Add("p_claim_token", claimToken, DbType.Guid);
        parameters.Add("p_error", error, DbType.String);

        var command = new CommandDefinition(
            commandText: "UPDATE outboxmessages SET error = :p_error WHERE id = :p_id AND claimtoken = :p_claim_token",
            parameters: parameters,
            transaction: _dapperDataContext.Transaction,
            commandType: CommandType.Text,
            cancellationToken: cancellationToken
        );

        await _dapperDataContext.Connection.ExecuteAsync(command).ConfigureAwait(false);
    }
}
