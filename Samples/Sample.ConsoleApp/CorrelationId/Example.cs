using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;

namespace ConsoleApp.CorrelationId;

public static class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine).UseMiddleware(new Middleware())
        );

        var rAction = functions
            .RegisterAction(
                "CorrelationIdExample",
                Result (string param, RScrapbook scrapbook, Context context) =>
                {
                    Console.WriteLine("Invocation mode: " + context.InvocationMode);
                    Console.WriteLine(CorrelationId.Get());
                    return Postpone.For(500);
                }).Invoke;

        CorrelationId.Set("some_id");
        _ = rAction(
            functionInstanceId: "",
            param: "hello world"
        );
        
        await BusyWait.Until(() =>
            store
                .GetFunction(new FunctionId("CorrelationIdExample", ""))
                .Map(sf => sf?.Status == Status.Succeeded)
        );

        Console.WriteLine("completed!!!");
    }

    private class Middleware : IPreCreationMiddleware
    {
        public Task<Result<TResult>> Invoke<TParam, TScrapbook, TResult>(
            TParam param, 
            TScrapbook scrapbook, 
            Context context, 
            Func<TParam, TScrapbook, Context, Task<Result<TResult>>> next
        ) where TParam : notnull where TScrapbook : RScrapbook, new()
        {
            if (!scrapbook.StateDictionary.ContainsKey("TriesLeft"))
                scrapbook.StateDictionary["TriesLeft"] = "...";
            else if (scrapbook.StateDictionary["TriesLeft"] == "")
                return new Result<TResult>(default(TResult)!).ToTask();

            scrapbook.StateDictionary["TriesLeft"] = scrapbook.StateDictionary["TriesLeft"][..^1];

            if (context.InvocationMode == InvocationMode.Retry)
            {
                Console.WriteLine($"Retry CorrelationId (before set): {CorrelationId.Get()}");
                CorrelationId.Set(scrapbook.StateDictionary["CorrelationId"]);
            }
            
            return next(param, scrapbook, context);
        }

        public Task PreCreation<TParam>(TParam param, Dictionary<string, string> stateDictionary, FunctionId functionId) where TParam : notnull
        {
            stateDictionary["CorrelationId"] = CorrelationId.Get();
            return Task.CompletedTask;
        }
    }
}