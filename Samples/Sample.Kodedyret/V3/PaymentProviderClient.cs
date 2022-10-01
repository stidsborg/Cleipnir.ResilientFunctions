using Serilog;

namespace Sample.Kodedyret.V3;

public interface IPaymentProviderClient
{
    Task Reserve(Guid transactionId, decimal amount);
    Task Capture(Guid transactionId);
    Task CancelReservation(Guid transactionId);
}

public class PaymentProviderClientStub : IPaymentProviderClient
{
    public Task Reserve(Guid transactionId, decimal amount)
        => Task.Delay(100).ContinueWith(_ =>
            Log.Logger.ForContext<IPaymentProviderClient>().Information($"PAYMENT_PROVIDER: Reserved '{amount}'")
        );
    
    public Task Capture(Guid transactionId) 
        => Task.Delay(100).ContinueWith(_ => 
            Log.Logger.ForContext<IPaymentProviderClient>().Information("PAYMENT_PROVIDER: Reserved amount captured")
        );
    public Task CancelReservation(Guid transactionId) 
        => Task.Delay(100).ContinueWith(_ => 
            Log.Logger.ForContext<IPaymentProviderClient>().Information("PAYMENT_PROVIDER: Reservation cancelled")
        );
}