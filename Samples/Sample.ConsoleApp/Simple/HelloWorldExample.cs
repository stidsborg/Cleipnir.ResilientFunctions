using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.Simple;

#pragma warning disable CS1998
public static class HelloWorldExample
{
    public static async Task Execute()
    {
        var store = new InMemoryFunctionStore();
        var functions = RFunctions.Create(store, unhandledExceptionHandler: Console.WriteLine);

        var f = functions.Register<string, string>(
            "hello world",
            async param => param.ToUpper()
        ).Invoke;

        var returned = await f("", "hello world").EnsureSuccess();
        Console.WriteLine($"1: {returned}");
    }
}
#pragma warning restore CS1998