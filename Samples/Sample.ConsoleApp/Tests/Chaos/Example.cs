using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.SqlServer;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.Data.SqlClient;

namespace ConsoleApp.Tests.Chaos;

public static class Example
{
    private static readonly object Sync = new();
    public static async Task Perform()
    {
        const int testSize = 100;
        var functionTypeId = "chaosTest".ToFunctionTypeId(); 
        var store = new SqlServerFunctionStore(CreateConnection, "chaos_test");
        var crashableStore = new CrashableFunctionStore(store);
        await store.DropIfExists();
        await store.Initialize();

        var exceptions = new List<Exception>();
        var firstRFunctions = RFunctions
            .Create(
                crashableStore,
                unhandledExceptionHandler: e => { lock (Sync) exceptions.Add(e); },
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.Zero
            );

        var firstRFunc = firstRFunctions.Register<int, string>(
            functionTypeId,
            async Task<Return<string>>(param) =>
            {
                await Task.Delay(1_000_000);
                return param.ToString();
            }
        ).Invoke;
        
        var secondRFunctions = RFunctions
            .Create(
                store,
                unhandledExceptionHandler: e => { lock (Sync) exceptions.Add(e); },
                crashedCheckFrequency: TimeSpan.FromMilliseconds(10),
                postponedCheckFrequency: TimeSpan.Zero
            );

        var secondRFunc = secondRFunctions.Register<int, string>(
            functionTypeId,
            async Task<Return<string>>(param) => param.ToString()
        ).Invoke;
        
        for (var i = 0; i < testSize; i++)
            _ = firstRFunc(i.ToString(), i);

        crashableStore.Crash();
        
        for (var i = 0; i < testSize; i++)
        {
            var result = await secondRFunc(i.ToString(), i).EnsureSuccess();
            var success = int.TryParse(result, out var j);
            if (!success || i != j)
                throw new Exception($"Expected: {i} Actual: {result}");
        }
        
        await BusyWait.Until(async 
            () => await store.GetFunctionsWithStatus(functionTypeId, Status.Executing).Map(s => !s.Any()),
            maxWait: TimeSpan.FromSeconds(10),
            checkInterval: TimeSpan.FromSeconds(1)
        );
        
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