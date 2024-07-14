using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;
using ConsoleApp.BankTransfer.Versioning;

namespace ConsoleApp.BankTransfer.Locking;

public static class Example
{
    public static async Task Perform()
    {
        var functionsRegistry = new FunctionsRegistry(new InMemoryFunctionStore());

        var actionRegistration = functionsRegistry
            .RegisterAction<Transfer>(
                flowType: nameof(TransferFunds),
                TransferFunds.Perform
            );

        var transfer = new Transfer(
            TransferId: Guid.NewGuid(),
            FromAccount: "FAccount",
            FromAccountTransactionId: Guid.NewGuid(),
            ToAccount: "TAccount",
            ToAccountTransactionId: Guid.NewGuid(),
            Amount: 100
        );
        await actionRegistration.Invoke(
            transfer.TransferId.ToString(),
            transfer
        );
    }
}