using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.SqlServer;
using Microsoft.Data.SqlClient;

namespace ConsoleApp.StressTest;

public static class Example
{
    private static readonly object Sync = new();
    public static async Task Perform()
    {
        var store = new SqlServerFunctionStore(CreateConnection, "stress_test");
        await store.Initialize();
        await store.Truncate();

        var exceptions = new List<Exception>();
        var rFunctions = RFunctions
            .Create(
                store,
                e =>
                {
                    lock (Sync)
                        exceptions.Add(e);
                },
                TimeSpan.FromMilliseconds(10_000),
                postponedCheckFrequency: TimeSpan.Zero
            );

        var rFunc = rFunctions.Register<int, string>(
            "stresstest",
            async Task<RResult<string>>(int param) =>
            {
                await Task.Delay(1);
                return param.ToString();
            }
        ).Invoke;

        var tasks = new List<Tuple<int, Task<RResult<string>>>>();
        for (var i = 0; i < 10_000; i++)
        {
            var task = rFunc(i.ToString(), i);
            tasks.Add(Tuple.Create(i, task));
        }

        foreach (var (_, task) in tasks)
            await task;

        Console.WriteLine("COMPLETED");
        Console.WriteLine($"EXCEPTIONS: {exceptions.Count}");
    }

    private static async Task<SqlConnection> CreateConnection()
    {
        const string connectionString = "Server=localhost;Database=master;User Id=sa;Password=Pa55word!";
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }
}