using System.Collections.Concurrent;
using System.Diagnostics;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class TimeoutSuspensionTest
{
    public static async Task<TestResult> Perform(IEngine helper)
    {
        const int testSize = 1_000;
        var testName = "TIMEOUT_SUSPENSION_TEST";
        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();

        var executionTimes = new ConcurrentBag<TimeSpan>();
        
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );        
        var registration = functionsRegistry.RegisterParamless(
            "TimeoutSuspensionTest",
            async Task (workflow) =>
            {
                var functionStopWatch = Stopwatch.StartNew();
                try
                {
                    await workflow.Message<string>(waitFor: TimeSpan.FromSeconds(30));
                }
                finally
                {
                    executionTimes.Add(functionStopWatch.Elapsed);
                }
            }
        );
        
        Console.WriteLine($"{testName}: Initializing");
        var instances = Enumerable.Range(0, testSize).Select(i => i.ToString().ToFlowInstance()).ToList();
        await registration.BulkSchedule(instances);
        
        Console.WriteLine($"{testName}: Waiting for instances to suspend");
        await BusyWait.Until(async () =>
            {
                var suspended = await store.GetExpiredFunctions(DateTime.UtcNow.Ticks).SelectAsync(f => f.Count);
                if (suspended == testSize)
                    return true;

                Console.WriteLine($"{testName}: Suspended {suspended} / {testSize}");
                await Task.Delay(250);
                return false;
            },
            maxWait: TimeSpan.FromSeconds(360)
        );
        stopWatch.Stop();
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        
        Console.WriteLine($"{testName} Appending messages to functions...");
        await registration.SendMessages(
            instances.Select(i => new BatchedMessage(i, "some message")).ToList()
        );
        
        Console.WriteLine($"{testName} Waiting for instances to complete...");
        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: $"{testName}: ");

        var averageExecutionTime = executionTimes.Select(t => t.Ticks).Sum() / executionTimes.Count;
        return new TestResult(insertionAverageSpeed, averageExecutionTime);
    }
}