namespace Sample.WebApi.OrderProcessing.Domain;

public record OrderAndPaymentProviderTransactionId(Order Order, Guid TransactionId);