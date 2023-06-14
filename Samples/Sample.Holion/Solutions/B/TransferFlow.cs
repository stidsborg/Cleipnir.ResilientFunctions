using Cleipnir.ResilientFunctions.Domain;
using Sample.Holion.B.BankTransfer;

namespace Sample.Holion.Solutions.B;

public sealed class TransferFlow : Flow<Transfer, TransferScrapbook>
{
    private IAccountService AccountService { get; }
    
    public TransferFlow(IAccountService accountService) => AccountService = accountService;
    
    public override async Task Run(Transfer transfer)
    {
        await using var @lock = await Utilities.Monitor.Acquire(
            "Account",
            transfer.FromAccount,
            Scrapbook.LockId,
            TimeSpan.FromSeconds(10)
        );
        if (@lock == null)
            throw new TimeoutException("Unable to acquire account lock within threshold of 10 seconds");

        var balance = await AccountService.GetCurrentBalance(transfer.FromAccount);
        if (balance - transfer.Amount < 0)
            throw new InvalidOperationException("Cannot transfer funds due to insufficient funds on sender account");
        
        var deductTask = Scrapbook.DoAtMostOnce(
            "DeductAmount",
            () => AccountService.PostTransaction(transfer.FromAccountTransactionId, transfer.FromAccount, -transfer.Amount)
        );
            
        var addTask = Scrapbook.DoAtMostOnce(
            "AddAmount",
            () => AccountService.PostTransaction(transfer.ToAccountTransactionId, transfer.ToAccount, transfer.Amount)
        );

        await Task.WhenAll(deductTask, addTask);
    }
}

public class TransferScrapbook : RScrapbook
{
    public string LockId { get; set; } = Guid.NewGuid().ToString();
} 