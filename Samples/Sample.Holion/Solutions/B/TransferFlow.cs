using Cleipnir.ResilientFunctions.Domain;
using Sample.Holion.B.BankTransfer;

namespace Sample.Holion.Solutions.B;

public sealed class TransferFlow : Flow<Transfer>
{
    private IAccountService AccountService { get; }
    
    public TransferFlow(IAccountService accountService) => AccountService = accountService;
    
    public override async Task Run(Transfer transfer)
    {
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