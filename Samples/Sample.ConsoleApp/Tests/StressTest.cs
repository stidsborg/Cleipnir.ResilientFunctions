using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.SqlServer;
using Microsoft.Data.SqlClient;

namespace ConsoleApp.Tests;

public static class StressTest
{
    private static readonly object Sync = new();
    public static async Task Perform()
    {
        var store = new SqlServerFunctionStore(CreateConnection, nameof(StressTest));
        await store.DropIfExists();
        await store.Initialize();

        var exceptions = new List<Exception>();
        var rFunctions = new RFunctions
            (
                store,
                unhandledExceptionHandler: e =>
                {
                    lock (Sync)
                        exceptions.Add(e);
                },
                crashedCheckFrequency: TimeSpan.FromMilliseconds(10_000),
                postponedCheckFrequency: TimeSpan.Zero
            );

        var rFunc = rFunctions.RegisterFunc<int, string>(
            functionTypeId: "stresstest",
            inner: async Task<string>(int param) =>
            {
                await Task.Delay(1);
                return param.ToString();
            }
        ).Invoke;

        var tasks = new List<Tuple<int, Task<string>>>();
        for (var i = 0; i < 10_000; i++)
        {
            var task = rFunc(i.ToString(), i);
            tasks.Add(Tuple.Create(i, task));
        }

        foreach (var (i, task) in tasks)
        {
            var str = await task;
            if (int.Parse(str) != i)
                Console.WriteLine("OH NO");
        }
            

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