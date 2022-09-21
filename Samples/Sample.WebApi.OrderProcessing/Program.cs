using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.AspNetCore.Postgres;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Dapper;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Sample.WebApi.OrderProcessing.Communication;
using Sample.WebApi.OrderProcessing.Communication.Messaging;
using Sample.WebApi.OrderProcessing.DataAccess;
using Sample.WebApi.OrderProcessing.RequestMiddleware;
using Sample.WebApi.OrderProcessing.RequestMiddleware.Asp;
using Serilog;
using Serilog.Events;
using Serilog.Formatting.Json;

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

        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
            .Enrich.FromLogContext()
            .WriteTo.Console(new JsonFormatter())
            .CreateLogger();
        
        Log.Logger.ForContext("Test", "testing").Information("HELLO WORLD");
        
        // Add services to the container.
        builder.Services.AddScoped<CorrelationId>();
        builder.Services.AddScoped<CorrelationIdMiddleware>();
        builder.Services.AddScoped<RequestMiddleware.ResilientFunctions.CorrelationIdMiddleware>();
        builder.Services.AddScoped<RequestMiddleware.ResilientFunctions.LoggingMiddleware>();
        builder.Services.AddSingleton(new SqlConnectionFactory(connectionString));
        builder.Services.AddSingleton<BusinessLogic.RpcBased.OrderProcessor>();
        builder.Services.AddScoped<BusinessLogic.RpcBased.OrderProcessor.Inner>();
        
        builder.Services.AddSingleton<BusinessLogic.MessageBased.OrderProcessor>();
        builder.Services.AddScoped<BusinessLogic.MessageBased.OrderProcessor.Inner>();
        
        builder.Services.TryAddSingleton<IHttpContextAccessor, HttpContextAccessor>();
        builder.Services.AddSingleton<IOrdersRepository, OrdersRepository>();
        
        builder.Services.AddSingleton<IPaymentProviderClient, PaymentProviderClientStub>();
        builder.Services.AddSingleton<IProductsClient, ProductsClientStub>();
        builder.Services.AddSingleton<IEmailClient, EmailClientStub>();
        builder.Services.AddSingleton<ILogisticsClient, LogisticsClientStub>();

        var messageQueue = CreateAndSetupMessageQueue();
        builder.Services.AddSingleton(messageQueue);
        
        builder.Services.AddRFunctionsService(
            connectionString,
            _ => new Options(
                unhandledExceptionHandler: rfe => Log.Logger.Error(rfe,"ResilientFrameworkException occured"),
                crashedCheckFrequency: TimeSpan.FromSeconds(1)
            ).UseMiddleware<RequestMiddleware.ResilientFunctions.CorrelationIdMiddleware>()
             .UseMiddleware<RequestMiddleware.ResilientFunctions.LoggingMiddleware>()
        );

        builder.Services.AddControllers();
        // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();
        builder.Host.UseSerilog();
        
        var app = builder.Build();
        
        // Configure the HTTP request pipeline.
        app.UseSwagger();
        app.UseSwaggerUI(options =>
        {
            options.SwaggerEndpoint("/swagger/v1/swagger.json", "Orders API");
            options.RoutePrefix = string.Empty;
        });

        app.UseMiddleware<CorrelationIdMiddleware>();
        
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