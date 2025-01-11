using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using ConsoleApp.BankTransfer.Versioning;

namespace ConsoleApp.BankTransfer.Locking;

public static class TransferFunds
{
    private static IBankCentralClient BankCentralClient { get; } = new BankCentralClient();
    
    public static async Task Perform(Transfer transfer, Workflow workflow)
    {
        await using var fromAccountLock = await workflow.Synchronization.AcquireLock("Account", transfer.FromAccount);
        await using var toAccountLock = await workflow.Synchronization.AcquireLock("Account", transfer.ToAccount);
        
        var deductTask = workflow.Effect.Capture(
            "DeductAmount",
            () => BankCentralClient
                .PostTransaction(
                    transfer.FromAccountTransactionId, 
                    transfer.FromAccount,
                    -transfer.Amount
                )
        );

        var addTask = workflow.Effect.Capture(
            "AddAmount",
            () => BankCentralClient.PostTransaction(
                transfer.ToAccountTransactionId, 
                transfer.ToAccount,
                transfer.Amount
            )
        );

        await Task.WhenAll(deductTask, addTask);
    }
}