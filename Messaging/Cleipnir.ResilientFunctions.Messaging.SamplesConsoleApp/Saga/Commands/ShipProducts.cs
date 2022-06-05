namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.Saga.Commands;

public record ShipProducts(string OrderId, string CustomerEmail, IEnumerable<string> ProductIds);