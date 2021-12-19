using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class TaskLinq
{
    public static Task<TOut> Map<TIn, TOut>(this Task<TIn> task, Func<TIn, TOut> f)
        => task.ContinueWith(t => f(t.Result));

    public static Task<bool> Any<T>(this Task<IEnumerable<T>> task) => task.ContinueWith(t => t.Result.Any());
}