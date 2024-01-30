using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using ConsoleApp.BankTransfer.Versioning;

namespace ConsoleApp.BankTransfer.Locking;

public static class TransferFunds
{
    private static IBankCentralClient BankCentralClient { get; } = new BankCentralClient();
    
    public static async Task Perform(Transfer transfer, Context context)
    {
        var monitor = context.Utilities.Monitor;
        
        var lockId = await context.Activities.Do("lockId", () => Guid.NewGuid().ToString());
        await using var _ = await monitor.Acquire(
            new LockInfo("Account", transfer.FromAccount, lockId),
            new LockInfo("Account", transfer.ToAccount, lockId)
        );

        var deductTask = context.Activities.Do(
            "DeductAmount",
            () => BankCentralClient
                .PostTransaction(
                    transfer.FromAccountTransactionId, 
                    transfer.FromAccount,
                    -transfer.Amount
                )
        );

        var addTask = context.Activities.Do(
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