using System.Diagnostics;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class MixingTest
{
    public static async Task<TestResult> Perform(IEngine helper)
    {
        const int testSize = 5000;

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        var start = DateTime.UtcNow.AddSeconds(30);
        Console.WriteLine("MIXING_TEST: Initializing");
        for (var i = 0; i < testSize; i++)
        {
            var functionId = new FunctionId("MixingTest", i.ToString());
            await store.CreateFunction(
                functionId,
                new StoredParameter(JsonSerializer.Serialize("hello world"), typeof(string).SimpleQualifiedName()),
                scrapbookType: null,
                crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks,
                version: 0
            );
            if (i % 2 == 0)
                await store.SetFunctionState(
                    functionId,
                    Status.Postponed,
                    scrapbookJson: null,
                    result: null,
                    errorJson: null,
                    postponedUntil: start.Ticks,
                    expectedEpoch: 0
                );
        }
        
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        Console.WriteLine($"MIXING_TEST: Initialization took: {stopWatch.Elapsed} with average speed (s): {insertionAverageSpeed}");

        while (true)
        {
            var startingIn = start - DateTime.UtcNow;
            Console.WriteLine("MIXING_TEST: Starting in: " + startingIn);
            if (startingIn < TimeSpan.FromSeconds(3)) break;
            await Task.Delay(500);
        }
        
        Console.WriteLine("MIXING_TEST: Waiting for invocations to begin");
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                UnhandledExceptionHandler: Console.WriteLine,
                CrashedCheckFrequency: TimeSpan.FromSeconds(1),
                PostponedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        var _ = rFunctions.RegisterAction(
            "MixingTest",
            void(string param) => { }
        );
        
        using var rFunctions2 = new RFunctions(
            store,
            new Settings(
                UnhandledExceptionHandler: Console.WriteLine,
                CrashedCheckFrequency: TimeSpan.FromSeconds(1),
                PostponedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        _ = rFunctions2.RegisterAction(
            "MixingTest",
            void(string param) => { }
        );

        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "MIXING_TEST: ");

        return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}