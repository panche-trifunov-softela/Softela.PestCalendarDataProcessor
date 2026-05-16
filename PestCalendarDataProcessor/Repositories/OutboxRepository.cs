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
                SELECT id FROM outbox_messages
                WHERE processed_at IS NULL
                  AND error IS NULL
                  AND (claimed_at IS NULL OR claimed_at < @stale_threshold)
                ORDER BY occurred_at
                LIMIT @batch_size
                FOR UPDATE SKIP LOCKED
            ),
            updated AS (
                UPDATE outbox_messages
                SET claimed_at = @claimed_at, claim_token = @claim_token
                FROM batch
                WHERE outbox_messages.id = batch.id
                RETURNING outbox_messages.id, event_type, payload, occurred_at, claimed_at, claim_token, processed_at, error
            )
            SELECT * FROM updated ORDER BY occurred_at
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
            commandText: "UPDATE outbox_messages SET processed_at = @p_processed_at WHERE id = @p_id AND claim_token = @p_claim_token",
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
            commandText: "UPDATE outbox_messages SET error = @p_error WHERE id = @p_id AND claim_token = @p_claim_token",
            parameters: parameters,
            transaction: _dapperDataContext.Transaction,
            commandType: CommandType.Text,
            cancellationToken: cancellationToken
        );

        await _dapperDataContext.Connection.ExecuteAsync(command).ConfigureAwait(false);
    }
}
