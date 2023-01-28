using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;

namespace ConsoleApp.BankTransfer;

public static class Example
{
    public static async Task Perform()
    {
        var rFunctions = new RFunctions(
            new InMemoryFunctionStore(),
            new Settings(dependencyResolver: new FuncDependencyResolver(_ => new BankCentralClient()))
        );

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