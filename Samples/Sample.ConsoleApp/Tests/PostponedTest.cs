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

namespace ConsoleApp.Tests;

public static class PostponedTest
{
    private static readonly object Sync = new();
    public static async Task Perform()
    {
        const int testSize = 100;
        var functionTypeId = nameof(PostponedTest).ToFunctionTypeId(); 
        var store = new SqlServerFunctionStore(CreateConnection, nameof(PostponedTest));
        var crashableStore = new CrashableFunctionStore(store);
        await store.DropIfExists();
        await store.Initialize();

        var exceptions = new List<Exception>();
        var firstRFunctions = new RFunctions
            (
                crashableStore,
                crashedCheckFrequency: TimeSpan.FromMilliseconds(10),
                postponedCheckFrequency: TimeSpan.Zero
            );

        var schedule = firstRFunctions
            .Func(
                functionTypeId,
                inner: Task<string>(int param) => param.ToString().ToTask()
            ).WithPostInvoke((_, _) => Postpone.For(2000, inProcessWait: false))
            .Register()
            .Schedule;
        
        _ = new RFunctions
            (
                store,
                unhandledExceptionHandler: e => { lock (Sync) exceptions.Add(e); },
                crashedCheckFrequency: TimeSpan.Zero,
                postponedCheckFrequency: TimeSpan.FromMilliseconds(500)
            ).Func<int, string>(
            functionTypeId,
            Task<string>(param) => 
                param.ToString().ToTask()
        ).Register();

        await Task.WhenAll(
            Enumerable
                .Range(0, testSize)
                .Select(i => schedule(i.ToString(), i))
        );
        crashableStore.Crash();
        
        await BusyWait.Until(async 
            () => await store
                    .GetFunctionsWithStatus(functionTypeId, Status.Succeeded)
                    .Map(s => s.Count() == testSize),
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