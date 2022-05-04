using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.StressTests;

public static class CrashedTest
{
    public static async Task Perform(IHelper helper)
    {
        const int testSize = 1000;

        await helper.InitializeDatabaseAndTruncateTable();
        var sqlStore = helper.CreateFunctionStore();

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

        Console.WriteLine("CRASHED_TEST: Waiting for invocations to begin");
        using var rFunctions = new RFunctions(
            sqlStore,
            Console.WriteLine,
            crashedCheckFrequency: TimeSpan.FromSeconds(1)
        );

        var _ = rFunctions.RegisterAction(
            "CrashedTest",
            void(string param) => { }
        );

        await WaitFor.AllCompleted(helper, logPrefix: "CRASHED_TEST: ");
    }
}