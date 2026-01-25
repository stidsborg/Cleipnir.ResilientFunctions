using System.Diagnostics;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class SuspensionTest
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
        var registration = functionsRegistry.RegisterAction(
            "SuspensionTest",
            async Task (string param, Workflow workflow) =>
            {
                await workflow.Message<object>();;
                await workflow.Effect.Capture(() => param);
            }
        );
        
        Console.WriteLine("SUSPENSION_TEST: Initializing");
        var instances = Enumerable.Range(0, testSize).Select(i => i.ToString()).ToList();
        await registration.BulkSchedule(
            instances: instances.Select(instance => new BulkWork<string>(instance, Param: instance))
        );

        var storedIds = instances.Select(i => StoredId.Create(registration.StoredType, i.ToString())).ToList();
        Console.WriteLine("SUSPENSION_TEST: Waiting for instances to suspend");
        await BusyWait.Until(async () =>
            {
                var suspended = (await store.GetFunctionsStatus(storedIds)).Count(s => s.Status == Status.Suspended);
                if (suspended == testSize)
                    return true;

                Console.WriteLine($"SUSPENSION_TEST: Suspended {suspended} / {testSize}");
                await Task.Delay(250);
                return false;
            },
            maxWait: TimeSpan.FromSeconds(360)
        );
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        
        Console.WriteLine("SUSPENSION_TEST: Appending messages to functions...");
        await registration.SendMessages(
            instances.Select(i => new BatchedMessage(i, "some message")).ToList()
        );
        
        Console.WriteLine("SUSPENSION_TEST: Waiting for instances to complete...");
        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "SUSPENSION_TEST: ");

        return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}