using System;
using System.Threading.Tasks;

namespace ConsoleApp;

internal static class Program
{
    private static async Task Main()
    {
        await WorkDistribution.Example.Perform();


        while (true)
        {
            var msg = await FetchNextMessage();
            await HandleMessage(msg);
            await AckMessage(msg);
        }
        
    }


    public static Task<object> FetchNextMessage() => throw new NotImplementedException();
    public static Task HandleMessage(object message) => throw new NotImplementedException();
    public static Task AckMessage(object message) => throw new NotImplementedException();
}