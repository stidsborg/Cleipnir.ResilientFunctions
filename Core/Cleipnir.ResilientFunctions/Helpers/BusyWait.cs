using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class BusyWait
{
    public static void Until(
        Func<bool> predicate, 
        bool throwOnThresholdExceeded = true, 
        TimeSpan? maxWait = null,
        TimeSpan? checkInterval = null
    )
    {
        checkInterval ??= TimeSpan.FromMilliseconds(1);
        maxWait ??= TimeSpan.FromSeconds(5);
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        while (stopWatch.Elapsed < maxWait)
        {
            if (predicate())
                return;
            
            Thread.Sleep(checkInterval.Value);
            // https://stackoverflow.com/questions/20082221/when-to-use-task-delay-when-to-use-thread-sleep
            //await Task.Delay(checkInterval.Value); Let's make this Async? Lets not tell the CPU to sleep. Let it work on other stuff
        }

        if (throwOnThresholdExceeded)
            throw new TimeoutException("Predicate was not meet within the threshold");
    }

    public static async Task ForeverUntilAsync(Func<bool> predicate, TimeSpan? checkInterval = null)
    {
        checkInterval ??= TimeSpan.FromMilliseconds(10);
        while (!predicate())
            await Task.Delay(checkInterval.Value);
    }
    
    public static async Task UntilAsync(
        Func<bool> predicate, 
        bool throwOnThresholdExceeded = true, 
        TimeSpan? maxWait = null,
        TimeSpan? checkInterval = null
    )
    {
        checkInterval ??= TimeSpan.FromMilliseconds(1);
        maxWait ??= TimeSpan.FromSeconds(5);
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        while (stopWatch.Elapsed < maxWait)
        {
            if (predicate())
                return;

            await Task.Delay(checkInterval.Value);
        }

        if (throwOnThresholdExceeded)
            throw new TimeoutException("Predicate was not meet within the threshold");
    }
    
    public static async Task Until(
        Func<Task<bool>> predicate, 
        bool throwOnThresholdExceeded = true, 
        TimeSpan? maxWait = null,
        TimeSpan? checkInterval = null
    )
    {
        checkInterval ??= TimeSpan.FromMilliseconds(1);
        maxWait ??= TimeSpan.FromSeconds(5);
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        while (stopWatch.Elapsed < maxWait)
        {
            if (await predicate())
                return;
                
            await Task.Delay(checkInterval.Value);
        }
            
        if (throwOnThresholdExceeded)
            throw new TimeoutException("Predicate was not meet within the threshold");
    }
}