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
            UPDATE OutboxMessages
            SET ClaimedAt = @claimed_at, ClaimToken = @claim_token
            WHERE Id IN (
                SELECT Id FROM OutboxMessages
                WHERE ProcessedAt IS NULL
                  AND Error IS NULL
                  AND (ClaimedAt IS NULL OR ClaimedAt < @stale_threshold)
                ORDER BY OccurredAt
                LIMIT @batch_size
                FOR UPDATE SKIP LOCKED
            )
            RETURNING Id, EventType, Payload, OccurredAt, ClaimedAt, ClaimToken, ProcessedAt, Error
            """;

        var command = new CommandDefinition(
            commandText: sql,
            parameters: parameters,
            transaction: _dapperDataContext.Transaction,
            commandType: CommandType.Text,
            cancellationToken: cancellationToken
        );

        var messages = await _dapperDataContext.Connection!.QueryAsync<OutboxMessage>(command).ConfigureAwait(false);
        return messages.ToList();
    }

    public async Task MarkAsProcessedAsync(long id, Guid claimToken, DateTimeOffset processedAt, CancellationToken cancellationToken = default)
    {
        var parameters = new DynamicParameters();
        parameters.Add("p_id", id, DbType.Int64);
        parameters.Add("p_claim_token", claimToken, DbType.Guid);
        parameters.Add("p_processed_at", processedAt, DbType.DateTimeOffset);

        var command = new CommandDefinition(
            commandText: "UPDATE OutboxMessages SET ProcessedAt = @p_processed_at WHERE Id = @p_id AND ClaimToken = @p_claim_token",
            parameters: parameters,
            transaction: _dapperDataContext.Transaction,
            commandType: CommandType.Text,
            cancellationToken: cancellationToken
        );

        await _dapperDataContext.Connection!.ExecuteAsync(command).ConfigureAwait(false);
    }

    public async Task MarkAsFailedAsync(long id, Guid claimToken, string error, CancellationToken cancellationToken = default)
    {
        var parameters = new DynamicParameters();
        parameters.Add("p_id", id, DbType.Int64);
        parameters.Add("p_claim_token", claimToken, DbType.Guid);
        parameters.Add("p_error", error, DbType.String);

        var command = new CommandDefinition(
            commandText: "UPDATE OutboxMessages SET Error = @p_error WHERE Id = @p_id AND ClaimToken = @p_claim_token",
            parameters: parameters,
            transaction: _dapperDataContext.Transaction,
            commandType: CommandType.Text,
            cancellationToken: cancellationToken
        );

        await _dapperDataContext.Connection!.ExecuteAsync(command).ConfigureAwait(false);
    }
}
