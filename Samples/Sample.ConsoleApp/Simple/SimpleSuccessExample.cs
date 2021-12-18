using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp;

public static class SimpleSuccessExample
{
    public static async Task Execute()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = RFunctions.Create(
           store,
           unhandledExceptionHandler: Console.WriteLine,
           crashedCheckFrequency: TimeSpan.Zero
        );

        var f = functions.Register<string, string>(
            nameof(SimpleSuccessExample).ToFunctionTypeId(),
            RFunc,
            s => s
        );

        var returned = await f("hello world");
        Console.WriteLine($"1: {returned}");
        
        returned = await f("hello world");
        Console.WriteLine($"2: {returned}");
    }

    private static async Task<RResult<string>> RFunc(string s)
    {
        await Task.Delay(1_000);
        return s.ToUpper();
    }
}