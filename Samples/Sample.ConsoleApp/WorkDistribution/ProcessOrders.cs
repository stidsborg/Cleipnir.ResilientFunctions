using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.WorkDistribution;

public static class ProcessOrders
{
    public static ActionRegistration<string>? ProcessOrder { get; set; }
    
    public static async Task Execute(List<string> orderIds, Workflow workflow)
    {
        var (effect, _, _) = workflow;
        await effect.Capture(
            "Log_ProcessingStarted",
            () => Console.WriteLine($"Processing of orders started ({orderIds.Count})")
        );

        await ProcessOrder!
            .BulkSchedule(orderIds.Select(id => new BulkWork<string>(id, id)))
            .Completion();
        
        await effect.Capture(
            "Log_ProcessingFinished",
            () => Console.WriteLine($"Processing of orders completed ({orderIds.Count})")
        );
    }
}