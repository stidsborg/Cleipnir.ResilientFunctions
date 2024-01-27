using System;
using System.Threading.Tasks;

namespace ConsoleApp.WorkDistribution;

public static class ProcessOrder
{
    public static async Task<string> Execute(string orderId)
    {
        await ReserveFunds(orderId);
        var trackingNumber = await ShipOrder(orderId);
        await CaptureFunds(orderId);
        return trackingNumber;
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