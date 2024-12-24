using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.ParallelEffect;

public static class Example
{
    public static async Task Do()
    {
        var store = new InMemoryFunctionStore();
        var functions = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler: Console.WriteLine));

        var rFunc = functions.RegisterParamless(
            flowType: "HelloWorld",
            inner: async workflow =>
            {
                var effect = workflow.Effect;
                
                var e1 = effect.Capture(async () =>
                {
                    await Task.Delay(10);
                    
                    var e11 = effect.Capture(async () =>
                    {
                        await Task.Delay(10);
                        Console.WriteLine(effect);
                        return "Hello world!";
                    });
                    var e12 = effect.Capture(async () =>
                    {
                        await Task.Delay(1);
                        Console.WriteLine(effect);
                        return "Hello again!";
                    });
                
                    await effect.WhenAny(e11, e12);
                    return "Hello world!";
                });
                var e2 = effect.Capture(async () =>
                {
                    await Task.Delay(1);
                    
                    var e21 = effect.Capture(async () =>
                    {
                        await Task.Delay(10);
                        Console.WriteLine(effect);
                        return "Hello world!";
                    });
                    var e22 = effect.Capture(async () =>
                    {
                        await Task.Delay(1);
                        Console.WriteLine(effect);
                        return "Hello again!";
                    });
                    
                    await effect.WhenAny(e21, e22);
                    return "Hello again!";
                });

                await effect.WhenAny(e1, e2);
            }
        );
        
        await rFunc.Invoke("some instance");
    }
}