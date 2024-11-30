using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.MariaDb;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Cleipnir.ResilientFunctions.SqlServer;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.WorkDistribution;

public static class Example
{
    public static async Task Perform()
    {
        const string postgresConnStr = "Server=localhost;Database=cleipnir_samples;User Id=postgres;Password=Pa55word!;Include Error Detail=true;";
        await Cleipnir.ResilientFunctions.PostgreSQL.DatabaseHelper.CreateDatabaseIfNotExists(postgresConnStr);
        var postgresStore = new PostgreSqlFunctionStore(postgresConnStr);
        await postgresStore.Initialize();
        await postgresStore.TruncateTables();

        const string sqlServerConnStr = "Server=localhost;Database=CleipnirSamples;User Id=sa;Password=Pa55word!;Encrypt=True;TrustServerCertificate=True;Max Pool Size=200;";
        await Cleipnir.ResilientFunctions.SqlServer.DatabaseHelper.CreateDatabaseIfNotExists(sqlServerConnStr);
        var sqlServerStore = new SqlServerFunctionStore(sqlServerConnStr);
        await sqlServerStore.Initialize();
        await sqlServerStore.TruncateTables();

        const string mariaDbConnStr = "server=localhost;userid=root;password=Pa55word!;database=cleipnir_samples;AllowPublicKeyRetrieval=True;";
        await Cleipnir.ResilientFunctions.MariaDb.DatabaseHelper.CreateDatabaseIfNotExists(mariaDbConnStr);
        var mariaDbStore = new MariaDbFunctionStore(mariaDbConnStr);
        await mariaDbStore.Initialize();
        await mariaDbStore.TruncateTables();
        
        Console.WriteLine();
        Console.WriteLine("Postgres: ");
        await Perform(postgresStore);
        Console.WriteLine();
        Console.WriteLine("SqlServer:");
        await Perform(sqlServerStore);
        Console.WriteLine();
        Console.WriteLine("MariaDB:");
        await Perform(mariaDbStore);

        Console.WriteLine();
        Console.WriteLine("All completed!");
    }

    private static async Task Perform(IFunctionStore store)
    {
        Console.WriteLine("Started: " + store.GetType().Name);
        
        await store.Initialize();
        var registry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler: Console.WriteLine));
        
        var processOrder = registry.RegisterAction<string>(
            "ProcessOrder",
            ProcessOrder.Execute
        );
        ProcessOrders.ProcessOrder = processOrder;
        var processOrders = registry.RegisterAction<List<string>>(
            "ProcessOrders",
            ProcessOrders.Execute
        );
        
        var orderIds = Enumerable
            .Range(100, 150)
            .Select(id => $"MK-{id}") 
            .ToList();
        
        await processOrders.Schedule("2024-01-27", orderIds).Completion();
    }
}