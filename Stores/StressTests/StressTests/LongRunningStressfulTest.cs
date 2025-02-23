using System.Diagnostics;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.StressTests.Engines;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class LongRunningStressfulTest
{
    public static async Task Perform(IEngine helper)
    {
        const int testSize = 1_000;

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();

        {
            using var functionsRegistry1 = new FunctionsRegistry(
                store,
                new Settings(unhandledExceptionHandler: e =>
                {
                    Console.WriteLine(e);
                    Environment.Exit(1);
                }, leaseLength: TimeSpan.FromSeconds(2), maxParallelRetryInvocations: testSize * 2)
            );
            var actionRegistration = functionsRegistry1.RegisterParamless(
                "LongRunningStressfulTest",
                async Task (workflow) =>
                {
                    Console.WriteLine(workflow.FlowId + " started");
                    var effect = workflow.Effect;
                    while (true)
                    {
                        for (var i = 0; i < 100; i++)
                        {
                            _ = await effect.Capture($"{i}", () => i);
                            await Task.Delay(10);
                        }
                    
                        for (var i = 0; i < 100; i++)
                        {
                            await effect.Clear($"{i}");
                            await Task.Delay(10);
                        }
                    }
                });
        
            Console.WriteLine("LONGRUNNING_TEST: Initializing");
            await actionRegistration.BulkSchedule(
                Enumerable.Range(1, testSize).Select(i => i.ToString().ToFlowInstance())
            );            
            
            Console.WriteLine("LONGRUNNING_TEST: Intiailization completed");
            Console.ReadLine();
        }
        
    }
}