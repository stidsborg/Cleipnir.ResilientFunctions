using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Helpers
{
    public static class TaskExtensions
    {
        public static async Task<List<T>> ToTaskList<T>(this Task<IEnumerable<T>> tasks) => new List<T>(await tasks);
        public static async Task<TOut> TaskSelect<TIn, TOut>(this Task<TIn> task, Func<TIn, TOut> selector)
            => selector(await task);

        public static async Task<T[]> RandomlyPermutate<T>(this Task<IEnumerable<T>> tasks) 
            => (await tasks).RandomlyPermutate();
        
        public static Task<T> ToTask<T>(this T t) => Task.FromResult(t);
        public static List<T> ToList<T>(this T t) => new List<T> {t};
        
        public static T? ToNullable<T>(this T t) => (T?) t;

        public static async Task<T> EnsureSuccess<T>(this Task<RResult<T>> task)
        {
            var result = await task;
            return result.EnsureSuccess();
        }
    }
}