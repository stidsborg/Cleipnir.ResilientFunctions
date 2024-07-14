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
        
        Console.WriteLine("CRASHED_TEST: Initializing");
        for (var i = 0; i < testSize; i++)
        {
            await store.CreateFunction(
                new FlowId("CrashedTest", i.ToString()),
                param: JsonSerializer.Serialize("hello world"),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            );
        }
        
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        Console.WriteLine($"CRASHED_TEST: Initialization took: {stopWatch.Elapsed} with average speed (s): {insertionAverageSpeed}");

        Console.WriteLine("CRASHED_TEST: Waiting for invocations to begin");
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                leaseLength: TimeSpan.FromSeconds(1)
            )
        );
        var _ = functionsRegistry.RegisterAction(
            "CrashedTest",
            void(string param) => { }
        );
        
        using var functionsRegistry2 = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                leaseLength: TimeSpan.FromSeconds(1)
            )
        );
        functionsRegistry2.RegisterAction(
            "CrashedTest",
            void(string param) => { }
        );

        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "CRASHED_TEST: ");

        return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}