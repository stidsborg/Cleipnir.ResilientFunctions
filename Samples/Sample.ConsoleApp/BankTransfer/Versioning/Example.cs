using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.BankTransfer.Versioning;

public static class Example
{
    public static async Task Perform()
    {
        var rFunctions = new RFunctions(new InMemoryFunctionStore());

        var transferSaga = new TransferSagaV1(rFunctions);
        var transfer = new Transfer(
            TransferId: Guid.NewGuid(),
            FromAccount: "FAccount",
            FromAccountTransactionId: Guid.NewGuid(),
            ToAccount: "TAccount",
            ToAccountTransactionId: Guid.NewGuid(),
            Amount: 100
        );
        await transferSaga.Perform(transfer);
    }
}