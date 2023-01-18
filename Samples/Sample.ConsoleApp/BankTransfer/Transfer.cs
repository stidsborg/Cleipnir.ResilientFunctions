using System;

namespace ConsoleApp.BankTransfer;

public record Transfer(
    Guid TransferId,
    string FromAccount,
    Guid FromAccountTransactionId,
    string ToAccount, 
    Guid ToAccountTransactionId,
    decimal Amount
);