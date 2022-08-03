using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.AspNetCore;
using Cleipnir.ResilientFunctions.Messaging.PostgreSQL;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Dapper;
using Sample.WebApi.OrderProcessing.Communication;
using Sample.WebApi.OrderProcessing.Communication.Messaging;
using Sample.WebApi.OrderProcessing.DataAccess;

namespace Sample.WebApi.OrderProcessing;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var port = args.Any() ? int.Parse(args[0]) : 5000;
        
        const string connectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=presentation;";
        //await DatabaseHelper.CreateDatabaseIfNotExists(connectionString);
        await DatabaseHelper.RecreateDatabase(connectionString);
        await InitializeTable(connectionString);
        var builder = WebApplication.CreateBuilder(args);

        // Add services to the container.
        builder.Services.AddSingleton(new SqlConnectionFactory(connectionString));
        builder.Services.AddSingleton<BusinessLogic.MessageBased.OrderProcessor>();
        builder.Services.AddSingleton<IOrdersRepository, OrdersRepository>();
        
        builder.Services.AddSingleton<IPaymentProviderClient, PaymentProviderClientStub>();
        builder.Services.AddSingleton<IProductsClient, ProductsClientStub>();
        builder.Services.AddSingleton<IEmailClient, EmailClientStub>();
        builder.Services.AddSingleton<ILogisticsClient, LogisticsClientStub>();

        var messageQueue = CreateAndSetupMessageQueue();
        builder.Services.AddSingleton(messageQueue);

        builder.Services.AddRFunctionsService(
            new PostgreSqlFunctionStore(connectionString),
            _ => new Settings(UnhandledExceptionHandler: Console.WriteLine)
        );
        builder.Services.AddEventSources(new PostgreSqlEventStore(connectionString));
        
        builder.Services.AddControllers();
        // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();

        var app = builder.Build();

        // Configure the HTTP request pipeline.
        app.UseSwagger();
        app.UseSwaggerUI(options =>
        {
            options.SwaggerEndpoint("/swagger/v1/swagger.json", "Orders API");
            options.RoutePrefix = string.Empty;
        });

        app.MapControllers();

        await app.RunAsync($"http://localhost:{port}");
    }

    private static async Task InitializeTable(string connectionString)
    {
        await using var conn = await SqlConnectionFactory.Create(connectionString);
        await conn.ExecuteAsync(@"
            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(255) PRIMARY KEY,
                products TEXT,
                customer_id UUID
            );"
        );
    }

    private static MessageBroker CreateAndSetupMessageQueue()
    {
        var messageBroker = new MessageBroker();
        _ = new EmailServiceStub(messageBroker);
        _ = new PaymentProviderStub(messageBroker);
        _ = new ProductsServiceStub(messageBroker);
        _ = new LogisticsServiceStub(messageBroker);
        return messageBroker;
    }
}