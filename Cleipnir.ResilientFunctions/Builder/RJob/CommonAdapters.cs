using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Builder.RJob;

public static class CommonAdapters
{
    public static Func<TScrapbook, Task> ToAsyncPreInvoke<TScrapbook>(
        Action<TScrapbook> preInvoke
    ) where TScrapbook : RScrapbook, new()
    {
        return scrapbook =>
        {
            preInvoke(scrapbook);
            return Task.CompletedTask;
        };
    }
    public static Func<Result, TScrapbook, Task<Result>> ToAsyncPostInvoke<TScrapbook>(
        Func<Result, TScrapbook, Result> postInvoke) where TScrapbook : RScrapbook, new()
    {
        return (result, scrapbook) =>
        {
            var postInvoked = postInvoke(result, scrapbook);
            return Task.FromResult(postInvoked);
        };
    }
}