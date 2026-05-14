using Softela.PestCalendarDataProcessor.Models;

namespace Softela.PestCalendarDataProcessor.Repositories;

public interface IOutboxRepository
{
    Task<List<OutboxMessage>> GetUnprocessedAsync(int batchSize, DateTimeOffset now, TimeSpan stalenessWindow, CancellationToken cancellationToken = default);
    Task MarkAsProcessedAsync(long id, Guid claimToken, DateTimeOffset processedAt, CancellationToken cancellationToken = default);
    Task MarkAsFailedAsync(long id, Guid claimToken, string error, CancellationToken cancellationToken = default);
}
