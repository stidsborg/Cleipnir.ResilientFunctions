using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils.Scrapbooks;

namespace ConsoleApp.Simple;

public class SimpleHttpExample
{
    public static async Task Do1()
    {
        var store = new InMemoryFunctionStore();
        var functions = RFunctions.Create(store, unhandledExceptionHandler: Console.WriteLine);
        
        var httpClient = new HttpClient();

        var rFunc = functions.Register(
            nameof(SimpleSuccessExample).ToFunctionTypeId(),
            async (string s) =>
            {
                var replies = new List<string>();
                for (var i = 0; i < 2; i++)
                {
                    var response = await httpClient.PostAsync(
                        "https://postman-echo.com/post",
                        new StringContent(s)
                    );
                    response.EnsureSuccessStatusCode();
                    var content = await response.Content.ReadAsStringAsync();
                    var echo = Regex.Match(content, @"""data"":""(?<echo>[\w\s]+)", RegexOptions.IgnoreCase).Groups["echo"].ToString();
                    replies.Add(echo);
                }
                return replies.ToSucceededRResult();
            },
            s => s
        ).Invoke;

        var response = await rFunc("hello resilient world!").EnsureSuccess();
        Console.WriteLine(string.Join(Environment.NewLine, response));
    }
    
    public static async Task DoWithScrapbook()
    {
        var store = new InMemoryFunctionStore();
        var rfunctions = 
            RFunctions.Create(store, unhandledExceptionHandler: Console.WriteLine);
        
        var httpClient = new HttpClient();

        var rFunc = rfunctions.Register(
            "pair-of-http-calls".ToFunctionTypeId(),
            async (string s, ListScrapbook<string> scrapbook) =>
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
                return scrapbook.List.ToSucceededRResult();
            },
            idFunc: s => s
        ).Invoke;

        var response = await rFunc("hello resilient world!").EnsureSuccess();
        Console.WriteLine(string.Join(Environment.NewLine, response));
    }
}