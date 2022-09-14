using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Invocation;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.CorrelationId;

public static class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new RFunctions(
            store,
            new Settings(UnhandledExceptionHandler: Console.WriteLine).RegisterMiddleware(new Middleware())
        );

        var rAction = functions
            .RegisterAction(
                "CorrelationIdExample",
                void(string param) => Console.WriteLine(param)
            ).Invoke;

        await rAction(
            functionInstanceId: "",
            param: "hello world",
            new RScrapbook {StateDictionary = new Dictionary<string, string> {{"CorrelationId", "id1"}}}
        );
    }

    private class Middleware : IMiddleware
    {
        public Task<Result<TResult>> Invoke<TParam, TScrapbook, TResult>(
            TParam param, 
            TScrapbook scrapbook, 
            Context context, 
            Func<TParam, TScrapbook, Context, Task<Result<TResult>>> next
        ) where TParam : notnull where TScrapbook : RScrapbook, new()
        {
            var correlationId = scrapbook.StateDictionary["CorrelationId"];
            Console.WriteLine($"CorrelationId: {correlationId}");

            return next(param, scrapbook, context);
        }
    }
}