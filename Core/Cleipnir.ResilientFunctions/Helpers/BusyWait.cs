﻿using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class BusyWait
{
    private static readonly TimeSpan DefaultMaxWait = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan DefaultCheckFrequency = TimeSpan.FromMicroseconds(5);
    
    public static void Until(
        Func<bool> predicate, 
        bool throwOnThresholdExceeded = true, 
        TimeSpan? maxWait = null,
        TimeSpan? checkInterval = null
    )
    {
        checkInterval ??= DefaultCheckFrequency;
        maxWait ??= DefaultMaxWait;
        
        var stopWatch = new Stopwatch();
        stopWatch.Start();
        
        while (stopWatch.Elapsed < maxWait)
        {
            if (predicate())
                return;
            
            Thread.Sleep(checkInterval.Value);
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
        checkInterval ??= DefaultCheckFrequency;
        maxWait ??= DefaultMaxWait;
        
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
        checkInterval ??= DefaultCheckFrequency;
        maxWait ??= DefaultMaxWait;
        
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