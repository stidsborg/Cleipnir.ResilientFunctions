using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.SqlServer;
using ConsoleApp.Utils;
using Microsoft.Data.SqlClient;

namespace ConsoleApp.Simple;

public static class SimpleFetchFunctionsWithStatus
{
    public static async Task Execute()
    {
        var functionTypeId = nameof(SimpleFetchFunctionsWithStatus).ToFunctionTypeId();
        var store = new SqlServerFunctionStore(CreateConnection, nameof(SimpleFetchFunctionsWithStatus));
        await store.Initialize();
        await store.Truncate();
        
        var functions = new RFunctions(
            store,
            unhandledExceptionHandler: Console.WriteLine,
            crashedCheckFrequency: TimeSpan.Zero
        );

        var f = functions.Register<string, string>(
            nameof(SimpleFetchFunctionsWithStatus).ToFunctionTypeId(),
            RFunc
        ).Invoke;

        await Safe.Try(async () => await f("a", "a"), Console.WriteLine);
        await Safe.Try(async () => await f("b", "b"), Console.WriteLine);
        await Safe.Try(async () => await f("c", "c"), Console.WriteLine);

        var statuses = await store
            .GetFunctionsWithStatus(functionTypeId, Status.Executing)
            .ToTaskList();

        Console.WriteLine(statuses);
    }

    private static async Task<Return<string>> RFunc(string s)
    {
        await Task.Delay(0);
        throw new Exception("oh no");
    }

    private static async Task<SqlConnection> CreateConnection()
    {
        var sqlConnection = new SqlConnection("Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!");
        await sqlConnection.OpenAsync();
        return sqlConnection;
    }
}