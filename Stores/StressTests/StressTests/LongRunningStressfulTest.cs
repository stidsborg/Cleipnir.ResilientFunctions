using System.Diagnostics;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.StressTests.Engines;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class LongRunningStressfulTest
{
    public static async Task Perform(IEngine helper)
    {
        const int testSize = 100;

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
                }, leaseLength: TimeSpan.FromSeconds(10), maxParallelRetryInvocations: testSize * 2)
            );
            var actionRegistration = functionsRegistry1.RegisterParamless(
                "LongRunningStressfulTest",
                async Task (workflow) =>
                {
                    var effect = workflow.Effect;
                    while (true)
                    {
                        for (var i = 0; i < 100; i++)
                        {
                            await effect.Capture(() => i);
                            await Task.Delay(10);
                        }
                    }
                });
        
            Console.WriteLine("LONGRUNNING_TEST: Initializing");
            var flowInstances = Enumerable
                .Range(1, testSize)
                .Select(i => i.ToString().ToFlowInstance())
                .ToList();

            await actionRegistration.BulkSchedule(flowInstances);
            Console.WriteLine("LONGRUNNING_TEST: Flows scheduled");
            Console.WriteLine("LONGRUNNING_TEST: Intiailization completed");
            Console.WriteLine();
            
            for (var i = 0;;i++)
            {
                var expiredFunctions = await store.GetExpiredFunctions(DateTime.UtcNow.Ticks);
                Console.WriteLine($"LONGRUNNING_TEST: Expired functions count #{i}: " + expiredFunctions.Count);
                if (expiredFunctions.Any())
                    return;
                await Task.Delay(1_000);
            }
        }
    }
}