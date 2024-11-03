using System.Diagnostics;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public class SuspensionTest
{
    public static async Task<TestResult> Perform(IEngine helper)
    {
        const int testSize = 1_000;

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );        
        var actionRegistration = functionsRegistry.RegisterAction(
            "SuspensionTest",
            async Task (string param, Workflow workflow) =>
                await workflow.Messages.First(TimeSpan.Zero)
        );
        
        Console.WriteLine("SUSPENSION_TEST: Initializing");
        for (var i = 0; i < testSize; i++)
            await actionRegistration.Schedule(i.ToString(), "hello world");
        
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        
        Console.WriteLine("SUSPENSION_TEST: Appending messages to functions...");
        _ = Task.Run(async () =>
        {
            for (var i = 0; i < testSize; i++)
                await actionRegistration.MessageWriters.For(i.ToString().ToStoredInstance()).AppendMessage("some message");
        });
        
        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "SUSPENSION_TEST: ");

        return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}