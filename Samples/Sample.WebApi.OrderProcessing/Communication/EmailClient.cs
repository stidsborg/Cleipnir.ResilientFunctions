namespace Sample.WebApi.OrderProcessing.Communication;

public interface IEmailClient
{
    Task SendOrderConfirmation(Guid customerId, IEnumerable<Guid> productIds);
}

public class EmailClientStub : IEmailClient
{
    public Task SendOrderConfirmation(Guid customerId, IEnumerable<Guid> productIds)
        => Task.Delay(Constants.ExternalServiceDelay).ContinueWith(_ => Console.WriteLine("EMAIL_SERVER: Order confirmation emailed"));
}