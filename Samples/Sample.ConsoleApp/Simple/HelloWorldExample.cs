using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.Simple;

public static class HelloWorldExample
{
    public static async Task Do()
    {
        var store = new InMemoryFunctionStore();
        var functions = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler: Console.WriteLine));

        var registration = functions.RegisterFunc(
            flowType: "HelloWorld",
            inner: (string param) => param.ToUpper().ToTask()
        );

        var returned = await registration.Invoke(flowInstance: "", param: "hello world");
        Console.WriteLine($"Returned: '{returned}'");
    }
}