using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Tests.Utils
{
    public static class BusyWait
    {
        public static void Until(Func<bool> predicate, bool throwOnThresholdExceeded = true)
        {
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            while (stopWatch.ElapsedMilliseconds < 10_000)
            {
                if (predicate())
                    return;
                
                Thread.Sleep(10);
            }

            if (throwOnThresholdExceeded)
                throw new TimeoutException("Predicate was not meet within the threshold");
        }
        
        public static async Task Until(Func<Task<bool>> predicate, bool throwOnThresholdExceeded = true)
        {
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            while (stopWatch.ElapsedMilliseconds < 10_000)
            {
                if (await predicate())
                    return;
                
                await Task.Delay(10);
            }
            
            if (throwOnThresholdExceeded)
                throw new TimeoutException("Predicate was not meet within the threshold");
        }
    }
}