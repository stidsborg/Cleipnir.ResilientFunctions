using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.AspNetCore.Postgres;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Serilog;
using Serilog.Events;

namespace Sample.Kodedyret;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var port = args.Any() ? int.Parse(args[0]) : 5000;
        
        const string connectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=kodedyret;";
        await DatabaseHelper.RecreateDatabase(connectionString);
        
        var builder = WebApplication.CreateBuilder(args);

        Log.Logger = new LoggerConfiguration()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
            .Enrich.FromLogContext()
            .WriteTo.Console()
            .CreateLogger();
        
        // Add services to the container.
        V0.IoCBindings.AddBindings(builder.Services);
        
        builder.Services.UseResilientFunctions( 
            connectionString,
            _ => new Options(
                unhandledExceptionHandler: rfe => Log.Logger.Error(rfe, "ResilientFrameworkException occured"),
                crashedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        
        builder.Host.UseSerilog();
        builder.Services.AddControllers();

        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();

        var app = builder.Build();
        
        app.UseSwagger();
        app.UseSwaggerUI();
        
        app.MapControllers();

        await app.RunAsync($"http://localhost:{port}");
    }
}