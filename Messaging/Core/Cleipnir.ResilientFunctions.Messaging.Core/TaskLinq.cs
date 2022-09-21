namespace Cleipnir.ResilientFunctions.Messaging.Core;

public static class TaskLinq
{
    public static async Task<TOut> Select<TIn, TOut>(this Task<TIn> task, Func<TIn, TOut> map)
    {
        var result = await task;
        return map(result);
    }
}