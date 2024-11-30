using System;
using System.Threading.Tasks;

namespace ConsoleApp;

internal static class Program
{
    private static async Task Main()
    {
        await WorkDistribution.Example.Perform();

        Console.ReadLine();
    }
}