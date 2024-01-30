using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.MySQL;

namespace ConsoleApp.WorkDistribution;

public static class Example
{
    public static async Task Perform()
    {
        //var store = new PostgreSqlFunctionStore("Server=localhost;Database=rfunctions;User Id=postgres;Password=Pa55word!; Include Error Detail=true;");
        //var store = new SqlServerFunctionStore("Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!;Encrypt=True;TrustServerCertificate=True;Max Pool Size=200;");
        //"server=localhost;userid=root;password=Pa55word!;database=rfunctions_tests;SSL Mode=None"
        var store = new MySqlFunctionStore("server=localhost;userid=root;password=Pa55word!;database=rfunctions;SSL Mode=None");
        //var store = new InMemoryFunctionStore();
        await store.Initialize();
        var datetime = DateTime.Now.Ticks;
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine, postponedCheckFrequency: TimeSpan.FromSeconds(1), leaseLength: TimeSpan.FromSeconds(5))
        );
        Console.WriteLine("Using datetime: " + datetime);
        var processOrder = functions.RegisterAction<ProcessOrderRequest>(
            "ProcessOrder" + datetime,
            ProcessOrder.Execute
        );
        ProcessOrders.ProcessOrder = processOrder;
        var processOrders = functions.RegisterAction<List<string>>(
            "ProcessOrders" + datetime,
            ProcessOrders.Execute
        );

        var orderIds = Enumerable
            .Range(0, 10)
            .Select(_ => Guid.NewGuid().ToString()) //Random.Shared.Next(1000, 9999)
            .ToList();
        await processOrders.Schedule("2024-01-27", orderIds);

        Console.ReadLine();
    }
}