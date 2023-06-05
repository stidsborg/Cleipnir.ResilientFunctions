using Cleipnir.ResilientFunctions.Domain;
using Sample.Holion.B.BankTransfer;

namespace Sample.Holion.Solutions.B;

public sealed class TransferFlow : Flow<Transfer>
{
    private IBankCentralClient BankCentralClient { get; }
    
    public TransferFlow(IBankCentralClient bankCentralClient) => BankCentralClient = bankCentralClient;
    
    public override async Task Run(Transfer transfer)
    {
        var deductTask = Scrapbook.DoAtMostOnce(
            "DeductAmount",
            () => BankCentralClient.PostTransaction(transfer.FromAccountTransactionId, transfer.FromAccount, -transfer.Amount)
        );
            
        var addTask = Scrapbook.DoAtMostOnce(
            "AddAmount",
            () => BankCentralClient.PostTransaction(transfer.ToAccountTransactionId, transfer.ToAccount, transfer.Amount)
        );

        await Task.WhenAll(deductTask, addTask);
    }
}