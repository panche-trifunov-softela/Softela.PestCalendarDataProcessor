namespace Softela.PestCalendarDataProcessor.Options;

public class OutboxOptions
{
    public int BatchSize { get; set; } = 10;
    public double StalenessWindowMinutes { get; set; } = 5;
    public string IntervalCron { get; set; } = "0 */1 * * * *";
}
