namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Clients;

public interface IPaymentProviderClient
{
    Task Reserve(Guid id, decimal amount);
    Task Capture(Guid id);
    Task CancelReservation(Guid id);
}

public class PaymentProviderClientStub : IPaymentProviderClient
{
    public Task Reserve(Guid id, decimal amount) => Task.CompletedTask;
    public Task Capture(Guid id) => Task.CompletedTask;
    public Task CancelReservation(Guid id) => Task.CompletedTask;
}