using Cleipnir.ResilientFunctions.StressTests.Common.Engines;

namespace Cleipnir.ResilientFunctions.StressTests.Common.FlippingTest;

public static class Test
{
    public static async Task Perform(IEngine helper)
    {
        var ready = new Queue<Node>();
        var started = new Queue<Node>();

        await helper.InitializeDatabaseAndInitializeAndTruncateTable();
        var store = await helper.CreateFunctionStore();

        Console.WriteLine("FLIPPING_TEST: Initializing...");
        for (var i = 0; i < 10; i++)
            ready.Enqueue(new Node(i, store));

        Console.WriteLine("FLIPPING_TEST: Starting first 3 nodes");
        for (var i = 0; i < 3; i++)
        {
            var node = ready.Dequeue();
            _ = node.Start();
            started.Enqueue(node);
        }

        while (ready.Count > 0)
        {
            Console.WriteLine("FLIPPING_TEST: Flipping");
            await Task.Delay(1000);
            var node = ready.Dequeue();
            _ = node.Start();
            started.Enqueue(node);
            started.Dequeue().Crash();
        }

        Console.WriteLine("FLIPPING_TEST: Flipped all nodes");
        Console.WriteLine("FLIPPING_TEST: Stopping remaining nodes");
        while (started.Count > 0)
            started.Dequeue().Stop();

        await WaitFor.AllCompleted(helper, logPrefix: "FLIPPING_TEST:");
        
        
    }
}