using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Softela.PestCalendarDataProcessor.Data;
using Softela.PestCalendarDataProcessor.Options;
using Softela.PestCalendarDataProcessor.Repositories;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((context, services) =>
    {
        var cfg = context.Configuration;

        // Dapper / PostgreSQL
        var connectionString = cfg["ConnectionStrings:OutboxDb"]
            ?? throw new InvalidOperationException("ConnectionStrings:OutboxDb is not configured.");

        services.AddScoped<IDapperDataContext>(_ => new DapperDataContext(connectionString));
        services.AddScoped<IOutboxRepository, OutboxRepository>();

        // Service Bus sender
        var serviceBusConnectionString = cfg["ServiceBus:ConnectionString"]
            ?? throw new InvalidOperationException("ServiceBus:ConnectionString is not configured.");
        var topicName = cfg["ServiceBus:TopicName"]
            ?? throw new InvalidOperationException("ServiceBus:TopicName is not configured.");

        services.AddSingleton(_ => new ServiceBusClient(serviceBusConnectionString));
        services.AddSingleton(sp => sp.GetRequiredService<ServiceBusClient>().CreateSender(topicName));

        // Outbox options
        services.Configure<OutboxOptions>(cfg.GetSection("Outbox"));
    })
    .Build();

host.Run();
