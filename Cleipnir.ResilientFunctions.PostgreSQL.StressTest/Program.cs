using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.PostgreSQL.StressTest;

internal static class Program
{
    private static async Task Main()
    {
        var ready = new Queue<Node>();
        var started = new Queue<Node>();

        const string connectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=rfunctions;";
        await DatabaseHelper.CreateDatabaseIfNotExists(connectionString);
        var sqlStore = new PostgreSqlFunctionStore(connectionString); 
        
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

        var confirmed = 0;
        while (true)
        {
            var executingFunctions = await sqlStore
                .GetExecutingFunctions("StressTest")
                .ToTaskList();
            Console.WriteLine("EXECUTING FUNCTIONS: " + executingFunctions.Count);
            
            var postponedFunctions = await sqlStore
                .GetPostponedFunctions("StressTest", expiresBefore: 0)
                .ToTaskList();
            Console.WriteLine("POSTPONED FUNCTIONS: " + postponedFunctions.Count);

            if (executingFunctions.Count == 0 && postponedFunctions.Count == 0)
            {
                confirmed++;
                if (confirmed == 3)
                    break;
            }
            else
                confirmed = 0;
                
            
            await Task.Delay(250);
        }
    }
}