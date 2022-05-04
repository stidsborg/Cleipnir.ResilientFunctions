using System.Diagnostics;

namespace Cleipnir.ResilientFunctions.StressTests;

public static class WaitFor
{
    public static async Task AllCompleted(IHelper helper, string logPrefix)
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        while (true)
        {
            var nonCompletes = await helper.NumberOfNonCompleted();

            Console.WriteLine($"{logPrefix} Non-completed: {nonCompletes}");
            await Task.Delay(250);

            if (nonCompletes == 0) break;
        }
        
        Console.WriteLine($"{logPrefix} Settled in: {stopWatch.Elapsed}");
    }
}