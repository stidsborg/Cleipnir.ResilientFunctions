namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.Domain;

public record Order(string OrderId, string CustomerEmail, IEnumerable<string> ProductIds);