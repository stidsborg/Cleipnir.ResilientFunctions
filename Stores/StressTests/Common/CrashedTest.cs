using System.Diagnostics;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.StressTests.Common;

public static class CrashedTest
{
    public static async Task Perform(IHelper helper)
    {
        const int testSize = 1000;

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var sqlStore = await helper.CreateFunctionStore();

        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        Console.WriteLine("CRASHED_TEST: Initializing");
        for (var i = 0; i < testSize; i++)
        {
            await sqlStore.CreateFunction(
                new FunctionId("CrashedTest", i.ToString()),
                new StoredParameter(JsonSerializer.Serialize("hello world"), typeof(string).SimpleQualifiedName()),
                scrapbookType: null,
                initialStatus: Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            );
        }
        
        stopWatch.Stop();
        Console.WriteLine("CRASHED_TEST: Initialization took: " + stopWatch.Elapsed);

        Console.WriteLine("CRASHED_TEST: Waiting for invocations to begin");
        using var rFunctions = new RFunctions(
            sqlStore,
            new Settings(
                UnhandledExceptionHandler: Console.WriteLine,
                CrashedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        var _ = rFunctions.RegisterAction(
            "CrashedTest",
            void(string param) => { }
        );
        
        using var rFunctions2 = new RFunctions(
            sqlStore,
            new Settings(
                UnhandledExceptionHandler: Console.WriteLine,
                CrashedCheckFrequency: TimeSpan.FromSeconds(1)
            )
        );
        _ = rFunctions2.RegisterAction(
            "CrashedTest",
            void(string param) => { }
        );

        await WaitFor.AllSuccessfullyCompleted(helper, testSize, logPrefix: "CRASHED_TEST: ");
    }
}