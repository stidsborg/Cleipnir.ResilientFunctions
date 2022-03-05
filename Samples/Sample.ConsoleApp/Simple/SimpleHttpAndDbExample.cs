using System;
using System.Collections.Generic;
using System.Data;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils.Scrapbooks;
using Dapper;

namespace ConsoleApp.Simple;

public class SimpleHttpAndDbExample
{
    private const string URL = "https://postman-echo.com/post";
    
    public static async Task Perform(IFunctionStore store, IDbConnection connection)
    {
        var functions = RFunctions.Create(store, unhandledExceptionHandler: Console.WriteLine);
        var httpClient = new HttpClient();

        var rFunc = functions.Register(
            "SimpleSaga",
            async (Guid id) =>
            {
                var response = await httpClient.PostAsync(URL, new StringContent(id.ToString()));
                response.EnsureSuccessStatusCode();
                var content = await response.Content.ReadAsStringAsync();
                await connection.ExecuteAsync("UPDATE Entity SET State=@State", new {State = content});
                return Return.Succeed;
            }
        ).Invoke;

        var id = Guid.NewGuid();
        await rFunc(id.ToString(), id).EnsureSuccess();
    }
    
    public static async Task DoWithScrapbook()
    {
        var store = new InMemoryFunctionStore();
        var rfunctions = 
            RFunctions.Create(store, unhandledExceptionHandler: Console.WriteLine);
        
        var httpClient = new HttpClient();

        var rFunc = rfunctions.Register<string, ListScrapbook<string>, List<string>>(
            "pair-of-http-calls".ToFunctionTypeId(),
            async (s, scrapbook) =>
            {
                for (var i = scrapbook.List.Count; i < 2; i++)
                {
                    var response = await httpClient
                        .PostAsync(
                            "https://postman-echo.com/post",
                            new StringContent(s)
                        );
                    response.EnsureSuccessStatusCode();
                    var content = await response.Content.ReadAsStringAsync();
                    var echo = Regex
                        .Match(
                            content, 
                            @"""data"":""(?<echo>[\w\s]+)",  
                            RegexOptions.IgnoreCase
                        ).Groups["echo"].ToString();
                    scrapbook.List.Add(echo);
                    await scrapbook.Save();
                }
                return scrapbook.List;
            }
        ).Invoke;

        var response = await rFunc("hello resilient world!", "hello resilient world!").EnsureSuccess();
        Console.WriteLine(string.Join(Environment.NewLine, response));
    }
}