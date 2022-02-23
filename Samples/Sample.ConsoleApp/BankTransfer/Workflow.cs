using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.BankTransfer;

public static class Workflow
{
    public static async Task Do()
    {
        var rFunctions = RFunctions.Create(new InMemoryFunctionStore());
        var transferSaga = new TransferSaga(rFunctions, new BankClient());
        var transfer = new Transfer(
            "FAccount",
            Guid.NewGuid(),
            "TAccount",
            Guid.NewGuid(),
            100
        ); 
        var result = await transferSaga.Perform(Guid.NewGuid().ToString(), transfer);
        result.EnsureSuccess();
    }
}