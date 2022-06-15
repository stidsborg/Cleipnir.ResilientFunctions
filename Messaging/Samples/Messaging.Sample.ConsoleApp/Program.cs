namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp
{
    internal static class Program
    {
        private static async Task Main()
        {
            await OrderProcessingFlow.Example.Perform();
            Console.WriteLine("PRESS ENTER TO EXIT");
            Console.ReadLine();
        }
    }
}