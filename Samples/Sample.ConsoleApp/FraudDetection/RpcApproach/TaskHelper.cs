using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ConsoleApp.FraudDetection.RpcApproach;

public static class TaskHelper
{
    public static Task<List<T>> AtLeastCompleted<T>(int count, params Task<T>[] tasks)
    {
        var sync = new object();
        var results = new List<T>(count);
        var tcs = new TaskCompletionSource<List<T>>();
        
        foreach (var task in tasks)
        {
            task.ContinueWith(t =>
            {
                lock (sync)
                {
                    results.Add(t.Result);
                    if (results.Count < count) return;
                }

                tcs.TrySetResult(results);
            });
        }

        return tcs.Task;
    } 
    
    public static Task<List<T>> AtLeastCompleted<T>(int count, TimeSpan withinTimeSpan, params Task<T>[] tasks)
    {
        var sync = new object();
        var results = new List<T>(count);
        var tcs = new TaskCompletionSource<List<T>>();

        var delayTask = Task.Delay(withinTimeSpan)
            .ContinueWith(_ =>
            {
                lock (sync)
                    if (results.Count < count)
                    {
                        tcs.TrySetException(new TimeoutException($"Not enough tasks completed before max delay: {withinTimeSpan}"));
                        return;
                    }

                tcs.TrySetResult(results);
            });

        foreach (var task in tasks)
        {
            task.ContinueWith(t =>
            {
                lock (sync)
                {
                    results.Add(t.Result);
                    if (results.Count < count) return;
                }

                tcs.TrySetResult(results);
                delayTask.Dispose();
            });
        }

        return tcs.Task;
    } 
    
    public static Task<List<T>> CompletesWithin<T>(TimeSpan withinTimeSpan, params Task<T>[] tasks)
    {
        var sync = new object();
        var timeout = false;
        var results = new List<T>(tasks.Length);
        var tcs = new TaskCompletionSource<List<T>>();

        var delayTask = Task.Delay(withinTimeSpan)
            .ContinueWith(_ =>
            {
                lock (sync)
                {
                    timeout = true;
                    tcs.TrySetResult(results);
                }
            });

        foreach (var task in tasks)
            task.ContinueWith(t =>
            {
                lock (sync)
                {
                    if (timeout) return;
                    results.Add(t.Result);
                    if (results.Count < tasks.Length) return;
                }

                tcs.TrySetResult(results);
                delayTask.Dispose();
            });

        return tcs.Task;
    } 
}