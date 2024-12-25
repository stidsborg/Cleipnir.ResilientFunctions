using System;
using System.Threading.Tasks;
using ConsoleApp.ParallelEffects;

namespace ConsoleApp;

internal static class Program
{
    private static async Task Main()
    {
        await Example.Do();

        Console.ReadLine();
    }
}