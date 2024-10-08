using System;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Helpers;

internal static class TaskExtensions
{
    [Pure]
    public static async Task<TOut> SelectAsync<TIn, TOut>(this Task<TIn> task, Func<TIn, TOut> selector)
        => selector(await task);
    [Pure]
    public static async Task<T> Flatten<T>(this Task<Task<T>> task)
        => await await task;
    [Pure]
    public static Task<T> ToTask<T>(this T t) => Task.FromResult(t);
    [Pure]
    public static T? ToNullable<T>(this T t) => (T?) t;
    
    public static async Task<TOut> AfterDo<TIn, TOut>(this Task<TIn> task, Func<TIn, TOut> work)
        => work(await task);
    
    public static async Task<TOut> AfterDo<TIn, TOut>(this Task<TIn> task, Func<TIn, Task<TOut>> work)
        => await work(await task);
    
    public static async Task AfterDo<TIn>(this Task<TIn> task, Func<TIn, Task> work)
        => await work(await task);
    
    public static async Task AfterDo<TIn>(this Task<TIn> task, Action<TIn> work)
        => work(await task);
}