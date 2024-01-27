using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Messaging;

namespace ConsoleApp.WorkDistribution;

public static class ProcessOrders
{
    public static FuncRegistration<string, string>? ProcessOrder { get; set; }
    
    public static async Task Execute(List<string> orderIds, Context context)
    {
        var (activities, _) = context;
        await activities.Do(
            "Log_ProcessingStarted",
            () => Console.WriteLine("Processing of orders started")
        );
        
        var orders = orderIds
            .Select(orderId => context.StartChild(ProcessOrder!, orderId, orderId))
            .ToList();
        
        await Task.WhenAll(orders);

        await activities.Do(
            "Log_ProcessingFinished",
            () => Console.WriteLine($"Processing of orders completed - total: '{orderIds.Count}'")
        );
    }
}