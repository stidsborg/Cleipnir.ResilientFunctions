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
            var storedParameter = new StoredParameter(
                ParamJson: JsonSerializer.Serialize("hello world"),
                ParamType: typeof(string).SimpleQualifiedName()
            );
            var storedScrapbook = new StoredScrapbook(
                ScrapbookJson: JsonSerializer.Serialize(new RScrapbook()),
                ScrapbookType: typeof(RScrapbook).SimpleQualifiedName()
            );
            var functionId = new FunctionId(nameof(PostponedTest), i.ToString());
            await store.CreateFunction(
                functionId,
                storedParameter,
                storedScrapbook,
                storedEvents: null,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            );
            await store.PostponeFunction(
                functionId,
                postponeUntil: start.Ticks,
                scrapbookJson: JsonSerializer.Serialize(new RScrapbook()),
                timestamp: DateTime.UtcNow.Ticks,
                expectedEpoch: 0,
                complementaryState: new ComplimentaryState.SetResult(storedParameter, storedScrapbook)
            );
        }
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        Console.WriteLine($"POSTPONED_TEST: Initialization took: {stopWatch.Elapsed} with average speed (s): {insertionAverageSpeed}");

        using var rFunctions1 = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                postponedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        rFunctions1.RegisterFunc(
            nameof(PostponedTest),
            int (string param) => 1 
        );

        using var rFunctions2 = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                postponedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        rFunctions2.RegisterFunc(
            nameof(PostponedTest),
            int (string param) => 2
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