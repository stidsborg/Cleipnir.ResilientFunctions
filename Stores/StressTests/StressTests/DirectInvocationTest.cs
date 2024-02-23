using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
using Cleipnir.ResilientFunctions.StressTests.Engines;
using Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests;

public class DirectInvocationTest
{
    public static async Task<TestResult> Perform(IEngine helper)
    {
        const int testSize = 1_000;

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();
        
        Console.WriteLine("DIRECT_INVOCATION_TEST: Starting now...");
        using var functionsRegistry = new FunctionsRegistry(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                leaseLength: TimeSpan.FromSeconds(10)
            )
        );
        var rFunc1 = functionsRegistry.RegisterFunc(
            "DirectInvocationTest",
            async Task<string> (string param, WorkflowState state, Workflow workflow) =>
            {
                try
                {
                    var messages = workflow.Messages;
                    await messages.AppendMessage(param);
                    
                    await state.Save();
                    return await messages.FirstOfType<string>();
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception);
                    throw;
                }
            }
        );
        
        using var functionsRegistry2 = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine, leaseLength: TimeSpan.FromSeconds(10))
        );
        var rFunc2 = functionsRegistry2.RegisterFunc(
            "DirectInvocationTest",
            async Task<string> (string param, WorkflowState state, Workflow workflow) =>
            {
                try
                {
                    var messages = workflow.Messages;
                    await messages.AppendMessage(param);
                    
                    await state.Save();
                    return await messages.FirstOfType<string>();
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception);
                    throw;
                }
            }
        );

        for (var i = 0; i < testSize; i++)
        {
            _ = Invoke(i % 2 == 0 ? rFunc1 : rFunc2, i);
        }

        var executionAverageSpeed = await 
            WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "DIRECT_INVOCATION_TEST: ");

        return new TestResult(0, executionAverageSpeed);
    }

    private static async Task Invoke(FuncRegistration<string, WorkflowState, string> funcRegistration, int i)
    {
        try
        {
            await funcRegistration.Invoke(i.ToString(), i.ToString());
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception);
        }
        
    }
}