using System.Diagnostics;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class PostponedTest
{
    public static async Task<TestResult> Perform(IEngine helper)
    {
        const int testSize = 1000;
        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();

        var start = DateTime.UtcNow.AddSeconds(30);
        Console.WriteLine("POSTPONED_TEST: Expected start: " + start);
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        Console.WriteLine("POSTPONED_TEST: Initializing");
        for (var i = 0; i < testSize; i++)
        {
            var storedParameter = JsonSerializer.Serialize("hello world");
            var functionId = new FlowId(nameof(PostponedTest), i.ToString());
            await store.CreateFunction(
                functionId,
                storedParameter,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            );
            await store.PostponeFunction(
                functionId,
                postponeUntil: start.Ticks,
                defaultState: null,
                timestamp: DateTime.UtcNow.Ticks,
                expectedEpoch: 0,
                complimentaryState: new ComplimentaryState(() => storedParameter, LeaseLength: 0)
            );
        }
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        Console.WriteLine($"POSTPONED_TEST: Initialization took: {stopWatch.Elapsed} with average speed (s): {insertionAverageSpeed}");

        using var functionsRegistry1 = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                watchdogCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        functionsRegistry1.RegisterFunc(
            nameof(PostponedTest),
            Task<int> (string param) => 1.ToTask() 
        );

        using var functionsRegistry2 = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                watchdogCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        functionsRegistry2.RegisterFunc(
            nameof(PostponedTest),
            Task<int> (string param) => 2.ToTask()
        );

        Console.WriteLine("POSTPONED_TEST: Waiting for invocations to begin");
        await Task.Delay(3000);

       while (true)
       {
           var startingIn = start - DateTime.UtcNow;
           Console.WriteLine("POSTPONED_TEST: Starting in: " + startingIn);
           if (startingIn < TimeSpan.FromSeconds(1)) break;
           await Task.Delay(500);
       }
       
       var executionAverageSpeed = await WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "POSTPONED_TEST:");
       return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}