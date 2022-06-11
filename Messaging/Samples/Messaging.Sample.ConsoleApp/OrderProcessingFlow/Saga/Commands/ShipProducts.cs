namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Saga.Commands;

public record ShipProducts(string OrderId, string CustomerEmail, IEnumerable<string> ProductIds);