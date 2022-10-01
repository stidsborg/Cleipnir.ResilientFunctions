namespace Sample.Kodedyret.V0;

public class IoCBindings
{
    public static void AddBindings(IServiceCollection serviceCollection)
    {
        serviceCollection.AddSingleton<OrderProcessor>();
        serviceCollection.AddSingleton<IEmailClient, EmailClientStub>();
        serviceCollection.AddSingleton<ILogisticsClient, LogisticsClientStub>();
        serviceCollection.AddSingleton<IPaymentProviderClient, PaymentProviderClientStub>();
    }
}