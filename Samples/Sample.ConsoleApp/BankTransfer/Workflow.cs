using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.BankTransfer;

public static class Workflow
{
    public static async Task Do()
    {
        var rFunctions = new FunctionContainer(new InMemoryFunctionStore());
        var transferSaga = new TransferSaga(rFunctions, new BankClient());
        var transfer = new Transfer(
            "FAccount",
            Guid.NewGuid(),
            "TAccount",
            Guid.NewGuid(),
            100
        ); 
        await transferSaga.Perform(Guid.NewGuid().ToString(), transfer);
    }
}