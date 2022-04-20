using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.SqlServer.StressTest;

internal static class Program
{
    private static async Task Main()
    {
        var ready = new Queue<Node>();
        var started = new Queue<Node>();

        var sqlStore = new SqlServerFunctionStore("Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!");
        await sqlStore.DropIfExists();
        await sqlStore.Initialize();
        
        Console.WriteLine("Stress test started...");
        for (var i = 0; i < 10; i++)
            ready.Enqueue(new Node(i, sqlStore));

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

        while (true)
        {
            var executingFunctions = await sqlStore.GetFunctionsWithStatus(
                "StressTest",
                Status.Executing
            ).ToTaskList();
            Console.WriteLine("EXECUTING FUNCTIONS: " + executingFunctions.Count);
            
            var postponedFunctions = await sqlStore.GetFunctionsWithStatus(
                "StressTest",
                Status.Postponed
            ).ToTaskList();
            Console.WriteLine("POSTPONED FUNCTIONS: " + postponedFunctions.Count);

            if (executingFunctions.Count == 0 && postponedFunctions.Count == 0)
                break;
            
            await Task.Delay(250);
        }
    }
}