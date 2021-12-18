using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.SqlServer;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils;
using Microsoft.Data.SqlClient;
using Newtonsoft.Json;

namespace ConsoleApp;

public static class SimpleFetchPostponedFunctionsWithStatus
{
    public static async Task Execute()
    {
        var functionTypeId = nameof(SimpleFetchPostponedFunctionsWithStatus).ToFunctionTypeId();
        var store = new SqlServerFunctionStore(CreateConnection, nameof(SimpleFetchPostponedFunctionsWithStatus));
        await store.Initialize();
        await store.Truncate();
        
        var functions = RFunctions.Create(
            store,
            unhandledExceptionHandler: Console.WriteLine,
            crashedCheckFrequency: TimeSpan.Zero
        );

        var f = functions.Register<int, string>(
            nameof(SimpleFetchPostponedFunctionsWithStatus).ToFunctionTypeId(),
            RFunc,
            s => s
        );

        await Utils.SafeTry(async () => await f(0), Console.WriteLine);
        await Utils.SafeTry(async () => await f(1), Console.WriteLine);
        await Utils.SafeTry(async () => await f(2), Console.WriteLine);

        var statuses = await store
            .GetFunctionsWithStatus(functionTypeId, Status.Postponed, DateTime.Today.AddDays(1).ToUniversalTime().Ticks)
            .ToTaskList();

        Console.WriteLine(statuses);

        var function = await store.GetFunction(new FunctionId(functionTypeId, "1".ToFunctionInstanceId()));
        Console.WriteLine(JsonConvert.SerializeObject(function, Formatting.Indented));
    }

    private static async Task<RResult<string>> RFunc(int postponeForDays)
    {
        await Task.Delay(0);
        return Postpone.Until(DateTime.Today.AddDays(postponeForDays));
    }

    private static async Task<SqlConnection> CreateConnection()
    {
        var sqlConnection = new SqlConnection("Server=localhost;Database=rfunctions;User Id=sa;Password=Pa55word!");
        await sqlConnection.OpenAsync();
        return sqlConnection;
    }
}