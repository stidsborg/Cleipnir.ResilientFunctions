using System;
using System.Data;
using System.Net.Http;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Dapper;

namespace ConsoleApp.Simple;

public class SimpleHttpAndDbExample
{
    private const string URL = "https://postman-echo.com/post";
    
    public static async Task Perform(IDbConnection connection)
    {
        var store = new InMemoryFunctionStore();
        var functions = new RFunctions(store, unhandledExceptionHandler: Console.WriteLine);
        var httpClient = new HttpClient();

        var rAction = functions.Register(
            "SimpleSaga",
            async (Guid id) =>
            {
                var response = await httpClient.PostAsync(URL, new StringContent(id.ToString()));
                response.EnsureSuccessStatusCode();
                var content = await response.Content.ReadAsStringAsync();
                await connection.ExecuteAsync("UPDATE Entity SET State=@State WHERE Id=@Id", new {State = content, Id = id});
                return Return.Succeed;
            }
        ).Invoke;

        var id = Guid.NewGuid();
        await rAction(id.ToString(), id).EnsureSuccess();
    }
}