using Azure.Messaging.ServiceBus;
using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Softela.PestCalendarDataProcessor.Data;
using Softela.PestCalendarDataProcessor.Options;
using Softela.PestCalendarDataProcessor.Repositories;

DefaultTypeMap.MatchNamesWithUnderscores = true;

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
        var queueName = cfg["ServiceBus:QueueName"]
            ?? throw new InvalidOperationException("ServiceBus:QueueName is not configured.");

        services.AddSingleton(_ => new ServiceBusClient(serviceBusConnectionString));
        services.AddSingleton(sp => sp.GetRequiredService<ServiceBusClient>().CreateSender(queueName));

        // Outbox options
        services.Configure<OutboxOptions>(cfg.GetSection("Outbox"));
    })
    .Build();

host.Run();
