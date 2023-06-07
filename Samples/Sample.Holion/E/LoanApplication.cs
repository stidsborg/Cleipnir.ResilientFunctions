namespace Sample.Holion.E;

public record LoanApplication(string Id, Guid CustomerId, decimal Amount, DateTime Created);