using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp
{
    internal static class Program
    {
        private static async Task Main()
        {
            var store = new InMemoryFunctionStore();
            var settings = new Settings();
            settings.UseMiddleware(new Middleware());
            var rFunctions = new RFunctions(store, settings);

            var rAction = rFunctions.RegisterAction(
                "TEST",
                async Task(string param) =>
                {
                    await Task.Delay(1);
                    Console.WriteLine(param);
                }
            );

            await rAction.Invoke("instanceId", "hello world");
        }

        private class Middleware : IMiddleware
        {
            public async Task<Result<TResult>> Invoke<TParam, TScrapbook, TResult>(
                TParam param, 
                TScrapbook scrapbook, 
                Context context, 
                Func<TParam, TScrapbook, Context, Task<Result<TResult>>> next
            ) where TParam : notnull where TScrapbook : RScrapbook, new()
            {
                Console.WriteLine("Before");
                var result = await next(param, scrapbook, context);
                Console.WriteLine("After");
                return result;
            }
        }
    }
}