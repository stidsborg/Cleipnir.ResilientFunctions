using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

namespace ConsoleApp.WorkDistribution;

public static class ProcessOrder
{
    public static async Task Execute(string orderId, Workflow workflow)
    {
        Console.WriteLine($"{orderId}: Started processing order");
        
        await ReserveFunds(orderId);
        var trackAndTraceNumber = await ShipOrder(orderId);
        await CaptureFunds(orderId);
        await EmailOrderConfirmation(orderId, trackAndTraceNumber);
        
        Console.WriteLine($"{orderId}: Finished processing order");
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

    private static async Task EmailOrderConfirmation(string orderId, string trackAndTraceNumber)
    {
        await Task.Delay(Random.Shared.Next(250, 1000));
        Console.WriteLine($"{orderId}: Order confirmation sent with track and trace number '{trackAndTraceNumber}'");
    }
}