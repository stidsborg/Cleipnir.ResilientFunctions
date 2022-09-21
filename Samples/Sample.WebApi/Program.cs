using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Sample.WebApi.Saga;

namespace Sample.WebApi;

internal static class Program
{
    private static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Add services to the container.
        builder.Services.AddInMemoryRFunctionsService(
            s => new Options(
                unhandledExceptionHandler: 
                    exception => s.GetRequiredService<ILogger>().LogError(exception, "Unhandled RFunction Exception")
                ),
            gracefulShutdown: true
        );
        builder.Services.AddSingleton<BookingSaga>();

        builder.Services.AddControllers();
        // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
        builder.Services.AddEndpointsApiExplorer();
        builder.Services.AddSwaggerGen();

        var app = builder.Build();

        // Configure the HTTP request pipeline.
        if (app.Environment.IsDevelopment())
        {
            app.UseSwagger();
            app.UseSwaggerUI();
        }

        app.UseHttpsRedirection();

        app.UseAuthorization();

        app.MapControllers();

        await app.RunAsync();
    }
}