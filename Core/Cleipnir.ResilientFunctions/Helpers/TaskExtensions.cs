using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class TaskExtensions
{
    [Pure]
    public static async Task<List<T>> ToTaskAsync<T>(this Task<IEnumerable<T>> tasks) => new List<T>(await tasks);
    [Pure]
    public static async Task<TOut> SelectAsync<TIn, TOut>(this Task<TIn> task, Func<TIn, TOut> selector)
        => selector(await task);
    [Pure]
    public static Task<T> ToTask<T>(this T t) => Task.FromResult(t);
    [Pure]
    public static T? ToNullable<T>(this T t) => (T?) t;
}