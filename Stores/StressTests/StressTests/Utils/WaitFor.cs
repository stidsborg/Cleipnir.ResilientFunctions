using System.Diagnostics;
using Cleipnir.ResilientFunctions.StressTests.Engines;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

public static class WaitFor
{
    public static async Task<long> AllSuccessfullyCompleted(IEngine helper, int testSize, string logPrefix)
    {
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        while (true)
        {
            var completed = await helper.NumberOfSuccessfullyCompleted();
            Console.WriteLine($"{logPrefix} Completed: {completed}/{testSize}");
            await Task.Delay(250);

            if (completed >= testSize) break;
        }

        var averageSpeed = testSize * 1000 / stopWatch.ElapsedMilliseconds;
        Console.WriteLine($"{logPrefix} Average Speed (s): {averageSpeed}" );
        Console.WriteLine($"{logPrefix} Settled in: {stopWatch.Elapsed}");

        return averageSpeed;
    }
}