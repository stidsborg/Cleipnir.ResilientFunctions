using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using ConsoleApp.ParallelEffects;

namespace ConsoleApp;

internal static class Program
{
    private static async Task Main()
    {
        for (var i = 0; i < 100; i++)
        {
            var store = new InMemoryFunctionStore();
            var functions = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler: Console.WriteLine));

            var rFunc = functions.RegisterParamless(
                flowType: "Fun",
                inner: async (Workflow workflow) =>
                {
                    _ = Task.Run(async () =>
                    {
                        while (true)
                        {
                            Console.Clear();
                            Console.WriteLine(workflow.ExecutionTree());
                            await Task.Delay(250);
                        }
                    });
                    
                    var effect = workflow.Effect;
                        var e1 = effect.Capture(async () =>
                        {
                            await Task.Delay(1);

                            var e11 = effect.Capture("1.1", async () =>
                            {
                                await Task.Delay(1000);
                                return "1.1";
                            }, ResiliencyLevel.AtLeastOnceDelayFlush);
                            var e12 = effect.Capture("1.2",async () =>
                            {
                                await Task.Delay(3000);
                                return "1.2";
                            }, ResiliencyLevel.AtLeastOnceDelayFlush);
                            var e13 = effect.Capture("1.3",async () =>
                            {
                                await Task.Delay(5000);
                                return "1.3";
                            }, ResiliencyLevel.AtLeastOnceDelayFlush);

                            var toReturn = await Task.WhenAll(e11, e12, e13);
                            await Task.Delay(5000);
                            return toReturn;
                        },ResiliencyLevel.AtLeastOnceDelayFlush);

                        var e2 = effect.Capture(async () =>
                        {
                            await Task.Delay(1);

                            var e21 = effect.Capture("2.1", async () =>
                            {
                                await effect.Capture("2.1.1", () => Task.Delay(1000), ResiliencyLevel.AtLeastOnceDelayFlush);
                                await effect.Capture("2.1.2", () => Task.Delay(1000), ResiliencyLevel.AtLeastOnceDelayFlush);
                                await effect.Capture("2.1.3", () => Task.Delay(1000), ResiliencyLevel.AtLeastOnceDelayFlush);
                                await effect.Capture("2.1.4", () => Task.Delay(1000), ResiliencyLevel.AtLeastOnceDelayFlush);
                                await effect.Capture("2.1.5", () => Task.Delay(1000), ResiliencyLevel.AtLeastOnceDelayFlush);
                                await effect.Capture("2.1.6", () => Task.Delay(1000), ResiliencyLevel.AtLeastOnceDelayFlush);
                                await effect.Capture("2.1.7", () => Task.Delay(1000), ResiliencyLevel.AtLeastOnceDelayFlush);
                                await effect.Capture("2.1.8", () => Task.Delay(1000), ResiliencyLevel.AtLeastOnceDelayFlush);
                                return "2.1";
                            });
                            
                            var e22 = effect.Capture("2.2", async () =>
                            {
                                await Task.Delay(1000);
                                return "2.2";
                            }, ResiliencyLevel.AtLeastOnceDelayFlush);
                            
                            return await Task.WhenAll(e21, e22);
                        });

                        await e1;
                        await e2;
                        //Console.WriteLine(string.Join(", ", (await e1).Concat(await e2)));
                }
            );
        
            await rFunc.Schedule("some instance".ToFlowInstance());
            var cp = await rFunc.ControlPanel("some instance");
            //await cp!.WaitForCompletion();

            Console.ReadLine();
            /*
            while (true)
            {
                Console.Clear();
                await cp!.Refresh();
                Console.WriteLine(i++ + " Status: " + cp.Status);
                Console.WriteLine(cp!.Effects.EffectTree());
                await Task.Delay(250);
            }
            await cp!.Refresh();
            Console.WriteLine(i++ + " Status: " + cp.Status);
            Console.WriteLine(cp!.Effects.EffectTree());
            await Task.Delay(5000);
            Console.Clear();*/
        }
    }
}