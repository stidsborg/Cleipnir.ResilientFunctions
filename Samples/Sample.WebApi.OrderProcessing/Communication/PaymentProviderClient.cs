using Serilog;

namespace Sample.WebApi.OrderProcessing.Communication;

public interface IPaymentProviderClient
{
    Task Reserve(Guid customerId, Guid transactionId, decimal amount);
    Task Capture(Guid transactionId);
    Task CancelReservation(Guid transactionId);
}

public class PaymentProviderClientStub : IPaymentProviderClient
{
    public Task Reserve(Guid customerId, Guid transactionId, decimal amount)
        => Task.Delay(Constants.ExternalServiceDelay).ContinueWith(_ =>
            Log.Logger.ForContext<IPaymentProviderClient>().Information($"PAYMENT_PROVIDER: Reserved '{amount}'")
        );
    
    public Task Capture(Guid transactionId) 
        => Task.Delay(Constants.ExternalServiceDelay).ContinueWith(_ => 
            Log.Logger.ForContext<IPaymentProviderClient>().Information("PAYMENT_PROVIDER: Reserved amount captured")
        );
    public Task CancelReservation(Guid transactionId) 
        => Task.Delay(Constants.ExternalServiceDelay).ContinueWith(_ => 
            Log.Logger.ForContext<IPaymentProviderClient>().Information("PAYMENT_PROVIDER: Reservation cancelled")
        );
}