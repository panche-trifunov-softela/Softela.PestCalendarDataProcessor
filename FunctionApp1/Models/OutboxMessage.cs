namespace Softela.PestCalendarDataProcessor.Models;

public class OutboxMessage
{
    public long Id { get; set; }
    public string EventType { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
    public DateTimeOffset OccurredAt { get; set; }
    public DateTimeOffset? ClaimedAt { get; set; }
    public Guid? ClaimToken { get; set; }
    public DateTimeOffset? ProcessedAt { get; set; }
    public string? Error { get; set; }
}
