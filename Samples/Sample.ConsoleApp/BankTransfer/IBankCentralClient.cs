using System;
using System.Threading.Tasks;

namespace ConsoleApp.BankTransfer;

public interface IBankCentralClient
{
    Task<bool> PostTransaction(Guid transactionId, string account, decimal amount);
}

public class BankCentralClient : IBankCentralClient
{
    public Task<bool> PostTransaction(Guid transactionId, string account, decimal amount)
    {
        Console.WriteLine($"POSTING: {amount} to {account} account");
        return Task.Delay(1_000).ContinueWith(_ => true);
    }
}