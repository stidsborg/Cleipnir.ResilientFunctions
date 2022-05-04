using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.StressTests;

public static class PostponedTest
{
    public static async Task Perform(IHelper helper)
    {
        const int testSize = 1000;
        var store = helper.CreateFunctionStore();

        var start = DateTime.UtcNow.AddSeconds(10);
        Console.WriteLine("POSTPONED_TEST: Expected start: " + start);
        var startTicks = start.Ticks;
        Console.WriteLine("POSTPONED_TEST: Initializing");
        for (var i = 0; i < testSize; i++)
        {
            var functionId = new FunctionId(nameof(PostponedTest), i.ToString());
            await store.CreateFunction(
                functionId,
                new StoredParameter(JsonSerializer.Serialize("hello world"), typeof(string).SimpleQualifiedName()),
                scrapbookType: null,
                initialStatus: Status.Executing,
                initialEpoch: 0,
                initialSignOfLife: 0
            );
            await store.SetFunctionState(
                functionId,
                Status.Postponed,
                scrapbookJson: null,
                result: null,
                errorJson: null,
                postponedUntil: start.Ticks,
                expectedEpoch: 0
            );
        }
        
        using var rFunctions1 = new RFunctions(
            store,
            Console.WriteLine,
            postponedCheckFrequency: TimeSpan.FromSeconds(1)
        );

        rFunctions1.RegisterFunc(
            nameof(PostponedTest),
            int (string param) => 1 
        );
        
        using var rFunctions2 = new RFunctions(
            store,
            Console.WriteLine,
            postponedCheckFrequency: TimeSpan.FromSeconds(1)
        );

        rFunctions2.RegisterFunc(
            nameof(PostponedTest),
            int (string param) => 2
        );

        Console.WriteLine("POSTPONED_TEST: Waiting for invocations to begin");
        await Task.Delay(3000);

        var allSucceeded = WaitFor.AllCompleted(helper, logPrefix: "POSTPONED_TEST:");

        while (true)
        {
            var startingIn = start - DateTime.UtcNow;
            Console.WriteLine("POSTPONED_TEST: Starting in: " + startingIn);
            if (startingIn.Ticks < 0) break;
            await Task.Delay(250);
        }

        await allSucceeded;
    }
}