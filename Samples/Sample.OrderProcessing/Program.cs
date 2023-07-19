﻿using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Serilog;

namespace Sample.OrderProcessing;

internal static class Program
{
    public static async Task Main(string[] args)
    {
        Log.Logger = new LoggerConfiguration()
            .WriteTo.Console()
            .CreateLogger();

        var connStr = "Server=localhost;Database=rfunctions;User Id=postgres;Password=Pa55word!; Include Error Detail=true;";
        var store = new PostgreSqlFunctionStore(connStr);
        await store.DropIfExists();
        await store.Initialize();
        var rFunctions = new RFunctions(
            store,
            new Settings(
                unhandledExceptionHandler: e => Log.Logger.Error(e, "Unhandled framework exception occured"),
                signOfLifeFrequency: TimeSpan.FromSeconds(5)
            )
        );
        
        //await Rpc.Do.Execute(rFunctions);
        await Messaging.Do.Execute(rFunctions);
        Console.WriteLine("Press enter to exit");
        Console.ReadLine();
    }
}