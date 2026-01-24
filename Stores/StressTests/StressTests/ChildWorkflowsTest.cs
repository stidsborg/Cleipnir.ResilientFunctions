using System.Diagnostics;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public static class ChildWorkflowsTest
{
    public static async Task<TestResult> Perform(IEngine helper)
    {
        const int testSize = 250;

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();
        
        var parentFlowType = new FlowType("Parent");
        var childFlowType = new FlowType("Child");
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );
        var parentFunctionId = new FlowId(parentFlowType, "Parent");
        
        var childRegistration = functionsRegistry.RegisterAction(
            childFlowType,
            Task (string param) => Task.CompletedTask
        );
        var parentRegistration = functionsRegistry.RegisterAction(
            parentFunctionId.Type,
            async Task (string param, Workflow workflow) =>
                await childRegistration
                    .BulkSchedule(
                        Enumerable
                            .Range(0, testSize)
                            .Select(i => new BulkWork<string>(i.ToString(), i.ToString()))
                    )
                    .Completion()
        );
        
        Console.WriteLine("CHILD_WORKFLOWS_TEST: Starting parent-invocation");
        await parentRegistration.Schedule(parentFunctionId.Instance.Value, "SomeParam");
        
        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "CHILD_WORKFLOWS_TEST: ");

        return new TestResult(InsertionAverageSpeed: 0, executionAverageSpeed);
    }
}