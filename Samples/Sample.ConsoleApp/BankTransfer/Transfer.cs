using System;

namespace ConsoleApp.BankTransfer;

public record Transfer(
    string FromAccount,
    Guid FromAccountTransactionId,
    string ToAccount, 
    Guid ToAccountTransactionId,
    decimal Amount
);