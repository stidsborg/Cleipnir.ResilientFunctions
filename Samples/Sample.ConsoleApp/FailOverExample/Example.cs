using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.InnerDecorators;
using Cleipnir.ResilientFunctions.SqlServer;
using Microsoft.Data.SqlClient;

namespace ConsoleApp.FailOverExample;

public static class Example
{
    public static async Task Execute()
    {
        var store = new SqlServerFunctionStore(CreateConnection);
        await store.Initialize();
        await store.Truncate();

        var service1Task = Service1();
        var service2Task = Service2();

        await Task.WhenAll(service1Task, service2Task);
        Console.WriteLine("AWAITED BOTH");
    }

    private static async Task Service1()
    {
        var functions = new RFunctions(
            new SqlServerFunctionStore(CreateConnection),
            new Settings(
                UnhandledExceptionHandler: Console.WriteLine,
                CrashedCheckFrequency: TimeSpan.Zero
            )
        );

        var callApi = functions
            .RegisterFunc(
                "call.api".ToFunctionTypeId(),
                OnFailure.PostponeFor(
                    Task<string> (string s) => new ApiCaller(true, 1).CallApi(s),
                    10
                    )
                ).Invoke;

        _ = callApi("input", "input"); //will fail
        await Task.Delay(2_000);

        var output = await callApi("input", "input");
        Console.WriteLine($"[SERVICE1] Function Return Value: {output}");
    }

    private static async Task Service2()
    {
        var functions = new RFunctions(
            new SqlServerFunctionStore(CreateConnection),
            new Settings(
                UnhandledExceptionHandler: Console.WriteLine,
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(1_000),
                PostponedCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );

        var callApi = functions
            .RegisterFunc<string, string>(
                "call.api".ToFunctionTypeId(),
                new ApiCaller(false, 2).CallApi
            ).Invoke;

        await Task.Delay(2_000);

        var output = await callApi("input", "input");
        Console.WriteLine($"[SERVICE2] Function Return Value: {output}");
    }

    private static async Task<SqlConnection> CreateConnection()
    {
        const string connectionString = "Server=localhost;Database=master;User Id=sa;Password=Pa55word!";
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }
}