namespace Sample.Kodedyret.V4;

public record EventsAndCommands;
public record OrderConfirmationEmailSent(string OrderId, Guid CustomerId) : EventsAndCommands;

public record GetProductsTotalPrice(string OrderId, IEnumerable<Guid> ProductId) : EventsAndCommands;
public record ProductsTotalPrice(string OrderId, decimal TotalPrice) : EventsAndCommands;

public record ReserveFunds(string OrderId, decimal Amount, Guid TransactionId, Guid CustomerId) : EventsAndCommands;
public record FundsReserved(string OrderId) : EventsAndCommands;
public record ShipProducts(string OrderId, Guid CustomerId, IEnumerable<Guid> ProductIds) : EventsAndCommands;
public record ProductsShipped(string OrderId) : EventsAndCommands;

public record SendOrderConfirmationEmail(string OrderId, Guid CustomerId) : EventsAndCommands;

public record CaptureFunds(string OrderId, Guid TransactionId) : EventsAndCommands;
public record FundsCaptured(string OrderId) : EventsAndCommands;
public record CancelFundsReservation(string OrderId, Guid TransactionId) : EventsAndCommands;
public record FundsReservationCancelled(string OrderId) : EventsAndCommands;