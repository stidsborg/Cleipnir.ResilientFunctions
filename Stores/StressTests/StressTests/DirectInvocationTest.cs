using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
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
        using var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                crashedCheckFrequency: TimeSpan.FromSeconds(1),
                suspensionCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        var rFunc1 = rFunctions.RegisterFunc(
            "DirectInvocationTest",
            async Task<string> (string param, RScrapbook scrapbook, Context context) =>
            {
                try
                {
                    var eventSource = await context.EventSource;
                    await eventSource.AppendEvent(param);
                    scrapbook.StateDictionary["Param"] = param;
                    await scrapbook.Save();
                    return await eventSource.NextOfType<string>();
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception);
                    throw;
                }
            }
        );
        
        using var rFunctions2 = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler: Console.WriteLine,
                crashedCheckFrequency: TimeSpan.FromSeconds(1),
                suspensionCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        var rFunc2 = rFunctions2.RegisterFunc(
            "DirectInvocationTest",
            async Task<string> (string param, RScrapbook scrapbook, Context context) =>
            {
                try
                {
                    var eventSource = await context.EventSource;
                    await eventSource.AppendEvent(param);
                    scrapbook.StateDictionary["Param"] = param;
                    await scrapbook.Save();
                    return await eventSource.NextOfType<string>();
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

    private static async Task Invoke(RFunc<string, RScrapbook, string> rFunc, int i)
    {
        try
        {
            await rFunc.Invoke(i.ToString(), i.ToString());
        }
        catch (Exception exception)
        {
            Console.WriteLine(exception);
        }
        
    }
}