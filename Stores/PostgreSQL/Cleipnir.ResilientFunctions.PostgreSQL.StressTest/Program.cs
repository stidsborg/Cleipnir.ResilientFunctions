using Dapper;
using Npgsql;

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
        
        while (true)
        {
            await using var conn = new NpgsqlConnection(connectionString);
            conn.Open();
            var nonCompletes = await conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM rfunctions WHERE result_json IS NULL;");

            Console.WriteLine("Non-completed: " + nonCompletes);
            await Task.Delay(250);

            if (nonCompletes == 0) break;
        }
    }
}