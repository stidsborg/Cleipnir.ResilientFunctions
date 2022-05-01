namespace Cleipnir.ResilientFunctions.StressTests.FlippingTest;

public static class Test
{
    public static async Task Perform(IHelper helper)
    {
        var ready = new Queue<Node>();
        var started = new Queue<Node>();

        await helper.InitializeDatabaseAndTruncateTable();
        var store = helper.CreateFunctionStore();

        Console.WriteLine("Stress test started...");
        for (var i = 0; i < 10; i++)
            ready.Enqueue(new Node(i, store));

        Console.WriteLine("Starting first 3 nodes");
        for (var i = 0; i < 3; i++)
        {
            var node = ready.Dequeue();
            _ = node.Start();
            started.Enqueue(node);
        }

        while (ready.Count > 0)
        {
            Console.WriteLine("Flipping");
            await Task.Delay(1000);
            var node = ready.Dequeue();
            _ = node.Start();
            started.Enqueue(node);
            started.Dequeue().Crash();
        }

        Console.WriteLine("Flipped all nodes");
        Console.WriteLine("Stopping remaining nodes");
        while (started.Count > 0)
            started.Dequeue().Stop();

        await WaitFor.AllCompleted(helper, logPrefix: "FLIPPING_TEST: ", expectedMin: TimeSpan.FromSeconds(3), expectedMax: TimeSpan.FromSeconds(4));
    }
}