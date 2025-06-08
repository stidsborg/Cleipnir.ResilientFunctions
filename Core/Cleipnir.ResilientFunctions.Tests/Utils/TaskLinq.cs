using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class TaskLinq
{
    public static Task<TOut> Map<TIn, TOut>(this Task<TIn> task, Func<TIn, TOut> f)
        => task.ContinueWith(t => f(t.Result));

    public static Task<List<T>> ToListAsync<T>(this Task<IEnumerable<T>> task)
        => task.ContinueWith(t => t.Result.ToList());

    public static async Task<List<T>> AwaitAll<T>(this IEnumerable<Task<T>> tasks)
    {
        var results = new List<T>();
        foreach (var task in tasks)
            results.Add(await task);
        
        return results;
    }
    
    public static Task<bool> AnyAsync<T>(this Task<IEnumerable<T>> task) => task.ContinueWith(t => t.Result.Any());
    public static Task<bool> AnyAsync<T>(this Task<IReadOnlyList<T>> task) => task.ContinueWith(t => t.Result.Any());

    public static async Task<T> WithTimeout<T>(this Task<T> task, int thresholdMs)
    {
        await Task.WhenAny(task, Task.Delay(thresholdMs));
        if (task.IsCompleted)
            return await task;

        throw new TimeoutException("Task did not complete within threshold");
    }
    
    public static async Task WithTimeout(this Task task, int thresholdMs)
    {
        await Task.WhenAny(task, Task.Delay(thresholdMs));
        if (task.IsCompleted)
            await task;
        else
            throw new TimeoutException("Task did not complete within threshold");
    }
}