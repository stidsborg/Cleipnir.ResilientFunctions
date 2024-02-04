using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.MySQL;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Cleipnir.ResilientFunctions.SqlServer;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.WorkDistribution;

public static class Example
{
    public static async Task Perform()
    {
        var postgresStore = new PostgreSqlFunctionStore("Server=localhost;Database=rfunctions;User Id=postgres;Password=Pa55word!; Include Error Detail=true;");
        await postgresStore.DropIfExists();
        var sqlServerStore = new SqlServerFunctionStore("Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!;Encrypt=True;TrustServerCertificate=True;Max Pool Size=200;");
        await sqlServerStore.DropIfExists();
        var mySqlStore = new MySqlFunctionStore("server=localhost;userid=root;password=Pa55word!;database=rfunctions;SSL Mode=None");
        await mySqlStore.DropIfExists();
        
        Console.WriteLine();
        Console.WriteLine("Postgres: ");
        var postgresTask = Task.Run(() => Perform(postgresStore));
        Console.WriteLine();
        Console.WriteLine("SqlServer:");
        var sqlServerTask = Task.Run(() => Perform(sqlServerStore));
        Console.WriteLine();
        Console.WriteLine("MySql:");
        var mySqlTask = Task.Run(() => Perform(mySqlStore));

        await postgresTask;
        await sqlServerTask;
        await mySqlTask;

        Console.WriteLine("All completed!");
    }

    private static async Task Perform(IFunctionStore store)
    {
        Console.WriteLine("Started: " + store.GetType().Name);
        
        await store.Initialize();
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine, postponedCheckFrequency: TimeSpan.FromSeconds(60), leaseLength: TimeSpan.FromSeconds(5))
        );
        
        var processOrder = functions.RegisterAction<ProcessOrderRequest>(
            "ProcessOrder",
            ProcessOrder.Execute
        );
        ProcessOrders.ProcessOrder!.Value = processOrder;
        var processOrders = functions.RegisterAction<List<string>>(
            "ProcessOrders",
            ProcessOrders.Execute
        );

        var orderIds = Enumerable
            .Range(0, 150)
            .Select(_ => Guid.NewGuid().ToString()) //Random.Shared.Next(1000, 9999)
            .ToList();
        await processOrders.Schedule("2024-01-27", orderIds);

        await BusyWait.Until(async () =>
                (await store.GetFunctionStatus(new FunctionId(processOrders.TypeId, "2024-01-27")))!.Status
                ==
                Status.Succeeded,
            maxWait: TimeSpan.FromSeconds(60)
        );
        
        Console.WriteLine("Completed: " + store.GetType().Name);
    }
}