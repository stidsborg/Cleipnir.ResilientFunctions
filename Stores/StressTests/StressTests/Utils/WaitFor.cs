using System.Diagnostics;
using Cleipnir.ResilientFunctions.StressTests.Engines;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

public static class WaitFor
{
    public static async Task AllCompleted(IEngine helper, int testSize, string logPrefix)
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        var sum = 0;
        var counts = 0;

        var curr = -1;

        while (true)
        {
            var nonCompletes = await helper.NumberOfNonCompleted();
            Console.WriteLine($"{logPrefix} Non-completed: {nonCompletes}");
            await Task.Delay(250);

            if (nonCompletes == 0) break;

            var prev = curr;
            curr = nonCompletes;
            
            if (prev == -1 || curr == testSize) continue;
            sum += prev - curr;
            counts++;
        }

        if (counts == 0) counts = 1;
        Console.WriteLine($"Average Speed: {sum/counts}" );
        Console.WriteLine($"{logPrefix} Settled in: {stopWatch.Elapsed}");
    }
    
    public static async Task<int> AllSuccessfullyCompleted(IEngine helper, int testSize, string logPrefix)
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        var iterations = 1;

        while (true)
        {
            var completed = await helper.NumberOfSuccessfullyCompleted();
            Console.WriteLine($"{logPrefix} Completed: {completed}/{testSize}");
            await Task.Delay(250);

            if (completed == testSize) break;
            
            iterations++;
        }

        var averageSpeed = testSize / iterations;
        Console.WriteLine($"{logPrefix} Average Speed: {averageSpeed}" );
        Console.WriteLine($"{logPrefix} Settled in: {stopWatch.Elapsed}");

        return averageSpeed;
    }
    
    public static async Task AllCompleted(IEngine helper, string logPrefix)
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