using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;
using Cleipnir.ResilientFunctions.Tests.Utils;

namespace ConsoleApp.Versioning;

public static class PaymentProviderExample
{
    private static readonly IFunctionStore Store = new InMemoryFunctionStore();
    
    public static async Task Do()
    {
        await Version1();
        await Version2();
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
            async Task (Order order) =>
            {
                await PaymentProviderV1.Reserve(order.TransactionId);
                await PaymentProviderV1.Capture(order.TransactionId);
                await NeverCompletingTask.OfVoidType;
                await SaveToDatabase(order);
            }).Schedule;
        
        await rAction(
            "order1",
            new Order(Version: 1, OrderNumber: "order#1", TransactionId: Guid.NewGuid())
        );
        crashableStore.Crash();
    }

    private static async Task Version2()
    {
        using var rFunctions = new RFunctions(
            Store, 
            new Settings(
                UnhandledExceptionHandler: Console.WriteLine,
                CrashedCheckFrequency: TimeSpan.FromMilliseconds(100)
            )
        );

        var rAction = rFunctions.RegisterAction(
            "SaveOrder",
            async Task(Order order) =>
            {
                if (order.Version == 1)
                {
                    await PaymentProviderV1.Reserve(order.TransactionId);
                    await PaymentProviderV1.Capture(order.TransactionId);
                }
                else // (order.Version == 2)
                {
                    await PaymentProviderV2.Reserve(order.TransactionId);
                    await PaymentProviderV2.Capture(order.TransactionId);
                }

                await SaveToDatabase(order);
                Console.WriteLine($"Completed order of type: '{order.GetType().Name}'");
            }
        ).Schedule;

        Console.WriteLine("Press enter to invoke V2");
        Console.ReadLine();
        await rAction("order#2", new Order(Version: 2, "order#2", TransactionId: Guid.NewGuid()));
        Console.WriteLine("Press enter to exit");
        Console.ReadLine();
    }

    private record Order(int Version, string OrderNumber, Guid TransactionId);

    private static Task SaveToDatabase(object document) => Task.CompletedTask;

    public static class PaymentProviderV1
    {
        public static Task Reserve(Guid transactionId) => Task.CompletedTask;
        public static Task Capture(Guid transactionId) => Task.CompletedTask;
        public static Task Cancel(Guid transactionId) => Task.CompletedTask;
    }

    public static class PaymentProviderV2
    {
        public static Task Reserve(Guid transactionId) => Task.CompletedTask;
        public static Task Capture(Guid transactionId) => Task.CompletedTask;
        public static Task Cancel(Guid transactionId) => Task.CompletedTask;
    }
}