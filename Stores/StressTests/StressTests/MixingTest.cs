﻿using System.Diagnostics;
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
            var storedParameter = new StoredParameter(
                ParamJson: JsonSerializer.Serialize("hello world"),
                ParamType: typeof(string).SimpleQualifiedName()
            );
            var storedScrapbook = new StoredScrapbook(
                ScrapbookJson: JsonSerializer.Serialize(new RScrapbook()),
                ScrapbookType: typeof(RScrapbook).SimpleQualifiedName()
            );
            
            var functionId = new FunctionId("MixingTest", i.ToString());
            await store.CreateFunction(
                functionId,
                storedParameter,
                storedScrapbook,
                leaseExpiration: DateTime.UtcNow.Ticks,
                postponeUntil: null,
                timestamp: DateTime.UtcNow.Ticks
            );
            if (i % 2 == 0)
                await store.PostponeFunction(
                    functionId,
                    postponeUntil: start.Ticks,
                    scrapbookJson: JsonSerializer.Serialize(new RScrapbook()),
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch: 0,
                    complimentaryState: new ComplimentaryState(() => storedParameter, () => storedScrapbook, LeaseLength: 0)
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
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                leaseLength: TimeSpan.FromSeconds(1),
                postponedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        var _ = functionsRegistry.RegisterAction(
            "MixingTest",
            void(string param) => { }
        );
        
        using var functionsRegistry2 = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                leaseLength: TimeSpan.FromSeconds(1),
                postponedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        functionsRegistry2.RegisterAction(
            "MixingTest",
            void(string param) => { }
        );

        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "MIXING_TEST: ");

        return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}