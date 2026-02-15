using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.ParallelEffects;

public static class Example
{
    public static async Task Do()
    {
        var store = new InMemoryFunctionStore();
        var functions = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler: Console.WriteLine));

        var rFunc = functions.RegisterFunc(
            flowType: "ParallelEffects",
            inner: async (string param, Workflow workflow) =>
            {
                var effect = workflow.Effect;
                
                var e1 = effect.Capture(async () =>
                {
                    await Task.Delay(10);
                    
                    var e11 = effect.Capture(async () =>
                    {
                        await Task.Delay(10);
                        return "1.1";
                    });
                    var e12 = effect.Capture(async () =>
                    {
                        await Task.Delay(1);
                        return "1.2";
                    });
                
                    return await await Task.WhenAny(e11, e12);
                });
                
                var e2 = effect.Capture(async () =>
                {
                    await Task.Delay(1);
                    
                    var e21 = effect.Capture(async () =>
                    {
                        await Task.Delay(10);
                        return "2.1";
                    });
                    var e22 = effect.Capture(async () =>
                    {
                        await Task.Delay(1);
                        return "2.2";
                    });
                    
                    return await await Task.WhenAny(e21, e22);
                });

                var results = await Task.WhenAll(e1, e2);
                return string.Join(", ", results);
            }
        );
        
        var result = await rFunc.Run("some instance".ToFlowInstance(), "some param");
        Console.WriteLine($"Result: {result}");
    }
}