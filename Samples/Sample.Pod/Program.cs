﻿using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.SqlServer;

namespace Sample.Pod;

internal static class Program
{
    private static async Task Main()
    {
        await Task.CompletedTask;
        
        var sqlStore = new SqlServerFunctionStore("Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!");
        var store = new CrashableFunctionStore(sqlStore);

        var rFunctions = new RFunctions(
            store,
            Console.WriteLine,
            crashedCheckFrequency: TimeSpan.FromSeconds(2)
        );

        var rAction = rFunctions.RegisterActionWithScrapbook(
            "Pod",
            async Task(string param, Scrapbook scrapbook) =>
            {
                Console.WriteLine($"PARAM: {param}");
                Console.WriteLine($"SCRAPBOOK MESSAGE: {scrapbook.Message}");
                scrapbook.Message += "THOMAS ";
                await scrapbook.Save();
                await Task.Delay(100_000);
            }).Invoke;

        _ = rAction("id1", "input_param");
        
        Console.WriteLine("PRESS ENTER TO CRASH");
        Console.ReadLine();
        store.Crash();
    }
    
    private class Scrapbook : RScrapbook
    {
        public string Message { get; set; } = "";
    }
}