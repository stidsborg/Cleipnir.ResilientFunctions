namespace Sample.Kodedyret.V1;

public class IoCBindings
{
    public static void AddBindings(IServiceCollection serviceCollection)
    {
        serviceCollection.AddSingleton<OrderProcessor>();
        serviceCollection.AddScoped<OrderProcessor.Inner>();
        serviceCollection.AddSingleton<IEmailClient, EmailClientStub>();
        serviceCollection.AddSingleton<ILogisticsClient, LogisticsClientStub>();
        serviceCollection.AddSingleton<IPaymentProviderClient, PaymentProviderClientStub>();
    }
}