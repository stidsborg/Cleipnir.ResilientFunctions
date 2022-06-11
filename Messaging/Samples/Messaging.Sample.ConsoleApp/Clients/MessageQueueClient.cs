using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.Saga;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.Saga.Commands;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.Saga.Events;

namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.Clients;

public interface IMessageQueueClient
{
    Task Send(ShipProducts message);
}

public class MessageQueueClient : IMessageQueueClient
{
    public OrderProcessingSaga? Saga { get; set; }

    public async Task Send(ShipProducts message)
    {
        await Task.Delay(1000);
        var raisedEvent = new ProductsShipped(message.OrderId);
        await Saga!.DeliverAndProcessEvent(message.OrderId, raisedEvent, nameof(ProductsShipped));
    }
}