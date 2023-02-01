﻿using System.Diagnostics;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
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
        
        Console.WriteLine("SUSPENSION_TEST: Initializing");
        for (var i = 0; i < testSize; i++)
        {
            var functionId = new FunctionId("SuspensionTest", i.ToString()); 
            await store.CreateFunction(
                functionId,
                new StoredParameter(JsonSerializer.Serialize("hello world"), typeof(string).SimpleQualifiedName()),
                new StoredScrapbook(JsonSerializer.Serialize(new RScrapbook()), typeof(RScrapbook).SimpleQualifiedName()),
                crashedCheckFrequency: TimeSpan.FromSeconds(1).Ticks
            );
            await store.SuspendFunction(
                functionId,
                suspendUntilEventSourceCountAtLeast: 1,
                scrapbookJson: new RScrapbook().ToJson(),
                expectedEpoch: 0
            );

            await store.EventStore.AppendEvent(functionId, "hello world".ToJson(), typeof(string).SimpleQualifiedName());
        }
        
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        Console.WriteLine($"SUSPENSION_TEST: Initialization took: {stopWatch.Elapsed} with average speed (s): {insertionAverageSpeed}");

        Console.WriteLine("SUSPENSION_TEST: Waiting for invocations to begin");
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                suspensionCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        var _ = rFunctions.RegisterAction(
            "SuspensionTest",
            void(string param) => { }
        );
        
        using var rFunctions2 = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                crashedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        _ = rFunctions2.RegisterAction(
            "SuspensionTest",
            void(string param) => { }
        );

        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "SUSPENSION_TEST: ");

        return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}