namespace Sample.Holion.B.BankTransfer;

public record Transfer(
    Guid TransferId,
    string FromAccount,
    Guid FromAccountTransactionId,
    string ToAccount, 
    Guid ToAccountTransactionId,
    decimal Amount
);