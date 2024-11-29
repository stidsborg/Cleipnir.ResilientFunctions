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
        
        var flowType = new FlowType(nameof(PostponedTest));
        var storedType = await store.TypeStore.InsertOrGetStoredType(flowType);
        
        var start = DateTime.UtcNow.AddSeconds(30);
        Console.WriteLine("POSTPONED_TEST: Expected start: " + start);
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        Console.WriteLine("POSTPONED_TEST: Initializing");
        for (var i = 0; i < testSize; i++)
        {
            var storedParameter = JsonSerializer.Serialize("hello world");
            await store.CreateFunction(
                new StoredId(storedType, i.ToString().ToStoredInstance()),
                "humanInstanceId",
                storedParameter.ToUtf8Bytes(),
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks,
                parent: null
            );
            await store.PostponeFunction(
                new StoredId(storedType, i.ToString().ToStoredInstance()),
                postponeUntil: start.Ticks,
                timestamp: DateTime.UtcNow.Ticks,
                expectedEpoch: 0,
                complimentaryState: new ComplimentaryState(() => storedParameter.ToUtf8Bytes(), LeaseLength: 0)
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