using System.Diagnostics;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public class WorkDistributionTest
{
    public static async Task<TestResult> Perform(IEngine helper)
    {
        const int testSize = 250;

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );
        var parentFunctionId = new FunctionId("Parent", "Parent");
        var childRegistration = functionsRegistry.RegisterAction(
            "Child",
            async Task (string param, Workflow workflow) =>
                await workflow.PublishMessage(parentFunctionId, param, idempotencyKey: workflow.FunctionId.ToString())
        );
        var parentRegistration = functionsRegistry.RegisterAction(
            parentFunctionId.TypeId,
            async Task (string param, Workflow workflow) =>
            {
                var children = new List<Task>(testSize);
                for (var i = 0; i < testSize; i++)
                    children.Add(childRegistration.Schedule(i.ToString(), i.ToString()));

                await Task.WhenAll(children);

                await workflow.Messages.Take(testSize).Completion();
            }
        );
        
        Console.WriteLine("WORKDISTRIBUTION_TEST: Starting parent-invocation");
        await parentRegistration.Schedule(parentFunctionId.InstanceId.Value, "SomeParam");
        
        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "WORKDISTRIBUTION_TEST: ");

        return new TestResult(InsertionAverageSpeed: 0, executionAverageSpeed);
    }
}