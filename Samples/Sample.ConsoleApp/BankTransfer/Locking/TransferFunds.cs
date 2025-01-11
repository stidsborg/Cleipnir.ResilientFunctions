using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using ConsoleApp.BankTransfer.Versioning;

namespace ConsoleApp.BankTransfer.Locking;

public static class TransferFunds
{
    private static IBankCentralClient BankCentralClient { get; } = new BankCentralClient();
    
    public static async Task Perform(Transfer transfer, Workflow workflow)
    {
        var fromAccount = workflow.Semaphores.CreateLock("Account", transfer.FromAccount);
        var toAccount = workflow.Semaphores.CreateLock("Account", transfer.ToAccount);

        await using var fromAccountLock = await fromAccount.Acquire();
        await using var toAccountLock = await toAccount.Acquire();

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