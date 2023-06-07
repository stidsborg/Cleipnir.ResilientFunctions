namespace Sample.Holion.B.BankTransfer;

public interface IAccountService
{
    Task<bool> PostTransaction(Guid transactionId, string account, decimal amount);
}

public class AccountService : IAccountService
{
    public Task<bool> PostTransaction(Guid transactionId, string account, decimal amount)
    {
        Console.WriteLine($"POSTING: {amount} to {account} account");
        return Task.Delay(1_000).ContinueWith(_ => true);
    }
}