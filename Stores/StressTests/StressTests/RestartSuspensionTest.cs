using System.Diagnostics;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class RestartSuspensionTest
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
        var registration = functionsRegistry.RegisterParamless(
            "SuspensionTest",
            async Task (workflow) =>
            {
                await workflow.Messages.First(maxWait: TimeSpan.Zero);
                await workflow.Effect.Capture(() => Guid.NewGuid());
            }
        );
        
        Console.WriteLine("RESTART_SUSPENSION_TEST: Initializing");
        var instances = Enumerable.Range(0, testSize).Select(i => i.ToString()).ToList();
        foreach (var instance in instances)
            await registration.Invoke(
                instance,
                new InitialState(
                    [new MessageAndIdempotencyKey("Hello")],
                    [new InitialEffect(0, Guid.NewGuid())]
                )
            );
        
        var insertionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;

        var controlPanels = instances
            .Select(i => registration.ControlPanel(i).Result!)
            .ToList();
        
        Console.WriteLine("RESTART_SUSPENSION_TEST: Restart functions...");
        
        stopWatch.Restart();
        foreach (var controlPanel in controlPanels)
        {
            await controlPanel.Restart();
        }
        var executionAverageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
    
        return new TestResult(insertionAverageSpeed, executionAverageSpeed);
    }
}