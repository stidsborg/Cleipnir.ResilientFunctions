namespace Sample.WebApi.OrderProcessing.Domain;

public record Order(string OrderId, Guid CustomerId, IEnumerable<Guid> ProductIds);