using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;

namespace ConsoleApp.Versioning;

public static class BrandExample
{
    private static readonly IFunctionStore Store = new InMemoryFunctionStore();
    
    public static async Task Do()
    {
        await Version1();
        Version2();
    }
    
    private static async Task Version1()
    {
        var crashableStore = new CrashableFunctionStore(Store);
        using var rFunctions = new RFunctions(
            crashableStore, 
            new Settings(
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );

        var rAction = rFunctions.RegisterAction(
            "SaveOrder",
            async Task (OrderV1 order) =>
            {
                await NeverCompletingTask.OfVoidType;
                await SaveToDatabase(order);
            }).Schedule;
        
        await rAction(
            "order1",
            new OrderV1("order#1")
        );
        crashableStore.Crash();
    }

    private static void Version2()
    {
        var crashableStore = new CrashableFunctionStore(Store);
        using var rFunctions = new RFunctions(
            crashableStore, 
            new Settings(
                UnhandledExceptionHandler: Console.WriteLine,
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );

        rFunctions.RegisterAction(
            "SaveOrder",
            async Task(Order order) =>
            {
                var o = order switch
                {
                    OrderV1 orderV1 => new OrderV2(orderV1.OrderNumber, Brand.Microsoft),
                    OrderV2 orderV2 => orderV2,
                    _ => throw new ArgumentOutOfRangeException(nameof(order))
                };

                await SaveToDatabase(o);
                Console.WriteLine($"Completed order of type: '{order.GetType().Name}'");
            }
        );
        
        Console.ReadLine();
    }

    private record Order;
    private record OrderV1(string OrderNumber) : Order;
    private record OrderV2(string OrderNumber, Brand Brand) : Order;

    private static Task SaveToDatabase(object document) => Task.CompletedTask;

    public enum Brand
    {
        Microsoft = 0,
        IBM = 1
    }
}