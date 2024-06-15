﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace ConsoleApp.WorkDistribution;

public record ProcessOrderRequest(string OrderId, FunctionId SendResultTo);

public static class ProcessOrder
{
    public static async Task Execute(ProcessOrderRequest request, Workflow workflow)
    {
        var (orderId, sendResultTo) = request;
        await ReserveFunds(orderId);
        var trackingNumber = await ShipOrder(orderId);
        await CaptureFunds(orderId);

        await workflow.SendMessage(
            sendResultTo,
            new FunctionCompletion<string>(trackingNumber, workflow.FunctionId),
            idempotencyKey: workflow.FunctionId.ToString()
        );
    }

    private static async Task ReserveFunds(string orderId)
    {
        await Task.Delay(Random.Shared.Next(250, 1000));
        Console.WriteLine($"{orderId}: Reserve funds");
    }
    
    private static async Task<string> ShipOrder(string orderId)
    {
        await Task.Delay(Random.Shared.Next(250, 1000));
        var trackingNumber = Guid.NewGuid().ToString();
        Console.WriteLine($"{orderId}: Order shipped with tracking number '{trackingNumber}'");
        
        return trackingNumber;
    }
    
    private static async Task CaptureFunds(string orderId)
    {
        await Task.Delay(Random.Shared.Next(250, 1000));
        Console.WriteLine($"{orderId}: Funds captured");
    }
}