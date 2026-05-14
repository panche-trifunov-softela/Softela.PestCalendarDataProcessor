using Azure.Messaging.ServiceBus;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Softela.PestCalendarDataProcessor.Models;
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

        logger.LogInformation("Claimed {Count} outbox message(s) for publishing...", messages.Count);

        foreach (var message in messages)
        {
            try
            {
                var sbMessage = new ServiceBusMessage(message.Payload)
                {
                    MessageId = message.Id.ToString(),
                    Subject = message.EventType,
                    ContentType = "application/json"
                };

                await sender.SendMessageAsync(sbMessage, cancellationToken);
                await outboxRepository.MarkAsProcessedAsync(message.Id, message.ClaimToken!.Value, now, cancellationToken);

                logger.LogInformation("Published outbox message {Id} ({EventType}).", message.Id, message.EventType);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Failed to publish outbox message {Id} ({EventType}).", message.Id, message.EventType);
                await outboxRepository.MarkAsFailedAsync(message.Id, message.ClaimToken!.Value, ex.ToString(), cancellationToken);
            }
        }
    }
}
