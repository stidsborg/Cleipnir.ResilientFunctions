using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.WorkDistribution;

public static class Example
{
    public static async Task Perform()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var processOrder = functions.RegisterFunc<string, string>(
            "ProcessOrder",
            ProcessOrder.Execute
        );
        ProcessOrders.ProcessOrder = processOrder;
        var processOrders = functions.RegisterAction<List<string>>(
            "ProcessOrders",
            ProcessOrders.Execute
        );

        var orderIds = Enumerable.Range(0, 20).Select(_ => Random.Shared.Next(1000, 9999).ToString()).ToList();
        await processOrders.Schedule("2024-01-27", orderIds);

        var ordersControlPanel = await processOrders.ControlPanel("2024-01-27");
        await ordersControlPanel!.WaitForCompletion(allowPostponedAndSuspended: true);
    }
}