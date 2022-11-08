namespace Sample.Kodedyret.V4;

public class IoCBindings
{
    public static void AddBindings(IServiceCollection serviceCollection)
    {
        serviceCollection.AddSingleton<OrderProcessor>();
        serviceCollection.AddScoped<OrderProcessor.Inner>();

        var messageBroker = new MessageBroker();
        var paymentProviderStub = new PaymentProviderStub(messageBroker);
        var logisticsServiceStub = new LogisticsServiceStub(messageBroker);
        var emailServiceStub = new EmailServiceStub(messageBroker);

        serviceCollection.AddSingleton(messageBroker);
    }
}