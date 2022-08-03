namespace Sample.WebApi.OrderProcessing.Communication;

public interface IPaymentProviderClient
{
    Task Reserve(Guid transactionId, decimal amount);
    Task Capture(Guid transactionId);
    Task CancelReservation(Guid transactionId);
}

public class PaymentProviderClientStub : IPaymentProviderClient
{
    public Task Reserve(Guid transactionId, decimal amount) 
        => Task.Delay(Constants.ExternalServiceDelay).ContinueWith(_ => Console.WriteLine($"BANK: Reserved '{amount}'"));
    public Task Capture(Guid transactionId) 
        => Task.Delay(Constants.ExternalServiceDelay).ContinueWith(_ => Console.WriteLine("BANK: Reserved amount captured"));
    public Task CancelReservation(Guid transactionId) 
        => Task.Delay(Constants.ExternalServiceDelay).ContinueWith(_ => Console.WriteLine("BANK: Reservation cancelled"));
}