using System.Diagnostics;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class CrashedTest
{
    public static async Task<TestResult> Perform(IEngine helper)
    {
        const int testSize = 5000;

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();

        var stopWatch = new Stopwatch();
        stopWatch.Start();

        var flowType = new FlowType("CrashedTest");
        var storedType = await store.FlowTypeStore.InsertOrGetStoredType(flowType);
        
        Console.WriteLine("CRASHED_TEST: Initializing");
        for (var i = 0; i < testSize; i++)
        {
            await store.CreateFunction(
                StoredId.Create(storedType, i.ToString()),
                humanInstanceId: "humanInstanceId",
                param: JsonSerializer.Serialize("hello world").ToUtf8Bytes(),
                postponeUntil: 0,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null,
                owner: null
            );
        }
        
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        Console.WriteLine($"CRASHED_TEST: Initialization took: {stopWatch.Elapsed} with average speed (s): {insertionAverageSpeed}");

        Console.WriteLine("CRASHED_TEST: Waiting for invocations to begin");
        using var functionsRegistry = await FunctionsRegistry.CreateAndStart(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(50)
            ),
            r =>
            {
                var _ = r.RegisterAction(
                    flowType,
                    Task (string param) => Task.CompletedTask
                );
            }
        );
        
        using var functionsRegistry2 = await FunctionsRegistry.CreateAndStart(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                watchdogCheckFrequency: TimeSpan.FromMilliseconds(50)
            ),
            r =>
            {
                r.RegisterAction(
                    flowType,
                    Task (string param) => Task.CompletedTask
                );
            }
        );

        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "CRASHED_TEST: ");

        return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}