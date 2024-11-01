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
        await postgresStore.Initialize();
        await postgresStore.TruncateTables();
        var sqlServerStore = new SqlServerFunctionStore("Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!;Encrypt=True;TrustServerCertificate=True;Max Pool Size=200;");
        await sqlServerStore.Initialize();
        await sqlServerStore.TruncateTables();
        var mySqlStore = new MySqlFunctionStore("server=localhost;userid=root;password=Pa55word!;database=rfunctions;AllowPublicKeyRetrieval=True;");
        await mySqlStore.Initialize();
        await mySqlStore.TruncateTables();
        
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
            new Settings(unhandledExceptionHandler: Console.WriteLine, watchdogCheckFrequency: TimeSpan.FromSeconds(60), leaseLength: TimeSpan.FromSeconds(5))
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
        ProcessOrder.MessageWriters = processOrders.MessageWriters;

        var orderIds = Enumerable
            .Range(0, 150)
            .Select(_ => Guid.NewGuid().ToString()) //Random.Shared.Next(1000, 9999)
            .ToList();
        await processOrders.Schedule("2024-01-27", orderIds);

        var controlPanel = await processOrders.ControlPanel("2024-01-27");
        
        await BusyWait.Until(async () =>
        {
            await controlPanel!.Refresh();
            return controlPanel.Status == Status.Succeeded;
        }, maxWait: TimeSpan.FromSeconds(60));
        
        Console.WriteLine("Completed: " + store.GetType().Name);
    }
}