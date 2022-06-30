using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class TaskExtensions
{
    public static async Task<List<T>> ToTaskAsync<T>(this Task<IEnumerable<T>> tasks) => new List<T>(await tasks);
    public static async Task<TOut> SelectAsync<TIn, TOut>(this Task<TIn> task, System.Func<TIn, TOut> selector)
        => selector(await task);
    public static Task<T> ToTask<T>(this T t) => Task.FromResult(t);
    public static T? ToNullable<T>(this T t) => (T?) t;
}