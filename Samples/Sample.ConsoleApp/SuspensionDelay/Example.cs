using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.SuspensionDelay;

public class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        var functions = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler: Console.WriteLine));

        var rFunc = functions.RegisterAction<string>(
            flowType: "DelaySuspension",
            inner: SuspensionDelayWorkflow
        );

        await rFunc.Schedule("instanceId", "hello world");

        Console.WriteLine("Waiting for completion");
        Console.ReadLine();
    }

    private static async Task SuspensionDelayWorkflow(string param, Workflow workflow)
    {
        await workflow.Delay(TimeSpan.FromSeconds(5));
        Console.WriteLine("Completed!");
    }
}