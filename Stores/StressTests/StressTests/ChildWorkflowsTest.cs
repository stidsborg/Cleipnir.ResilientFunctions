using System.Diagnostics;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public class ChildWorkflowsTest
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

        ActionRegistration<string>? parentRegistration = null;
        var childRegistration = functionsRegistry.RegisterAction(
            childFlowType,
            async Task (string param, Workflow workflow) =>
                await parentRegistration!.MessageWriters
                    .For(parentFunctionId.Instance)
                    .AppendMessage(param, idempotencyKey: workflow.FlowId.ToString())
        );
        parentRegistration = functionsRegistry.RegisterAction(
            parentFunctionId.Type,
            async Task (string param, Workflow workflow) =>
            {
                await childRegistration.BulkSchedule(
                    Enumerable
                        .Range(0, testSize)
                        .Select(i => new BulkWork<string>(i.ToString(), i.ToString()))
                );
                
                await workflow.Messages.Take(testSize).Completion();
            }
        );
        
        Console.WriteLine("CHILD_WORKFLOWS_TEST: Starting parent-invocation");
        await parentRegistration.Schedule(parentFunctionId.Instance.Value, "SomeParam");
        
        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "CHILD_WORKFLOWS_TEST: ");

        return new TestResult(InsertionAverageSpeed: 0, executionAverageSpeed);
    }
}