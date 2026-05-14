using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Softela.PestCalendarDataProcessor.Options;
using Softela.PestCalendarDataProcessor.Repositories;

namespace Softela.PestCalendarDataProcessor;

public sealed class OutboxPublisherFunction(
    IOutboxRepository outboxRepository,
    ServiceBusSender sender,
    IOptions<OutboxOptions> outboxOptions,
    ILogger<OutboxPublisherFunction> logger)
{
    [Function(nameof(OutboxPublisherFunction))]
    public async Task Run([TimerTrigger("%OutboxIntervalCron%")] TimerInfo timer, CancellationToken cancellationToken)
    {
        var now = DateTimeOffset.UtcNow;
        var options = outboxOptions.Value;
        var stalenessWindow = TimeSpan.FromMinutes(options.StalenessWindowMinutes);

        var messages = await outboxRepository.GetUnprocessedAsync(options.BatchSize, now, stalenessWindow, cancellationToken);

        if (messages.Count == 0)
            return;

        logger.LogInformation("Claimed {Count} outbox message(s) for publishing.", messages.Count);

        foreach (var message in messages)
        {
            try
            {
                var sbMessage = new ServiceBusMessage((string)message.Payload)
                {
                    MessageId = ((long)message.Id).ToString(),
                    Subject = (string)message.EventType,
                    ContentType = "application/json"
                };

                await sender.SendMessageAsync(sbMessage, cancellationToken);
                await outboxRepository.MarkAsProcessedAsync((long)message.Id, (Guid)message.ClaimToken, now, cancellationToken);

                logger.LogInformation("Published outbox message {Id} ({EventType}).", (long)message.Id, (string)message.EventType);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Failed to publish outbox message {Id} ({EventType}).", (long)message.Id, (string)message.EventType);
                await outboxRepository.MarkAsFailedAsync((long)message.Id, (Guid)message.ClaimToken, ex.Message, cancellationToken);
            }
        }
    }
}
