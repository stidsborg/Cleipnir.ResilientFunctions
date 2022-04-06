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
    public static Func<Return, TScrapbook, Task<Return>> ToAsyncPostInvoke<TScrapbook>(
        Func<Return, TScrapbook, Return> postInvoke) where TScrapbook : RScrapbook, new()
    {
        return (returned, scrapbook) =>
        {
            var postInvoked = postInvoke(returned, scrapbook);
            return Task.FromResult(postInvoked);
        };
    }
}