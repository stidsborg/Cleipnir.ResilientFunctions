namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Domain;

public record Order(string OrderId, string CustomerEmail, IEnumerable<string> ProductIds);