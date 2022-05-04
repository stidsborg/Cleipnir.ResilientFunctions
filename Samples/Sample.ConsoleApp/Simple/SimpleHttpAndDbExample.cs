using System;
using System.Data;
using System.Net.Http;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;
using Dapper;

namespace ConsoleApp.Simple;

public static class SimpleHttpAndDbExample
{
    private const string URL = "https://google.com";
    
    public static async Task RegisterAndInvoke(IDbConnection connection, IFunctionStore store)
    {
        var functions = new RFunctions(store, new Settings(UnhandledExceptionHandler: Console.WriteLine));
        var httpClient = new HttpClient();

        var rAction = functions.RegisterAction(
            functionTypeId: "HttpAndDatabaseSaga",
            inner: async (Guid id) =>
            {
                var response = await httpClient.PostAsync(URL, new StringContent(id.ToString()));
                response.EnsureSuccessStatusCode();
                var content = await response.Content.ReadAsStringAsync();
                await connection.ExecuteAsync(
                    "UPDATE Entity SET State=@State WHERE Id=@Id",
                    new {State = content, Id = id}
                );
            }).Invoke;

        var id = Guid.NewGuid();
        await rAction(functionInstanceId: id.ToString(), param: id);
    }
}