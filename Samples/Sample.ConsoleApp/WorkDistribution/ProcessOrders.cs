using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.WorkDistribution;

public static class ProcessOrders
{
    public static AsyncLocal<ActionRegistration<ProcessOrderRequest>>? ProcessOrder { get; } = new();
    
    public static async Task Execute(List<string> orderIds, Workflow workflow)
    {
        var (effect, messages, _) = workflow;
        await effect.Capture(
            "Log_ProcessingStarted",
            () => Console.WriteLine("Processing of orders started")
        );

        await effect.Capture(
            "ScheduleOrders",
            async () =>
            {
                foreach (var orderId in orderIds)
                    await ProcessOrder!.Value!.Schedule(
                        functionInstanceId: orderId,
                        new ProcessOrderRequest(orderId, workflow.FunctionId)
                    );
            }
        );
        
        await messages
            .OfType<FunctionCompletion<string>>()
            .Take(orderIds.Count)
            .Completion();

        await effect.Capture(
            "Log_ProcessingFinished",
            () => Console.WriteLine($"Processing of orders completed - total: '{orderIds.Count}'")
        );
    }
}