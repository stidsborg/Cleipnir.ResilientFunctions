using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using static ConsoleApp.Utils.Safe;

namespace ConsoleApp.Simple;

public static class SimplePostponeExample
{
    public static async Task Execute()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new RFunctions(
           store,
           unhandledExceptionHandler: Console.WriteLine,
           crashedCheckFrequency: TimeSpan.Zero
        );

        var f = functions.Register<string, string>(
            nameof(SimpleSuccessExample).ToFunctionTypeId(),
            RFunc
        ).Invoke;

        await Try(async () => await f("hello world", "hello world"), Console.WriteLine);
        await Try(async () => await f("hello world", "hello world"), Console.WriteLine);
    }

    private static async Task<Return<string>> RFunc(string s)
    {
        await Task.Delay(100);
        return Postpone.For(TimeSpan.FromSeconds(10));
    }
}