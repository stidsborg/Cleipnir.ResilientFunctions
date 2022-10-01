namespace Sample.Kodedyret.V4;

public class IoCBindings
{
    public static void AddBindings(IServiceCollection serviceCollection)
    {
        serviceCollection.AddSingleton<OrderProcessor>();
        serviceCollection.AddScoped<OrderProcessor.Inner>();
    }
}