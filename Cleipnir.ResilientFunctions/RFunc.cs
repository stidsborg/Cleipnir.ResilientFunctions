using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RFunc
{
    public delegate Task<TReturn> Invoke<in TParam, TReturn>(string functionInstanceId, TParam param)
        where TParam : notnull;

    public delegate Task<TReturn> ReInvoke<TReturn>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null
    );

    public delegate Return<TReturn> SyncPostInvoke<TParam, TScrapbook, TReturn>(
        Return<TReturn> returned, 
        TScrapbook scrapbook, 
        Metadata<TParam> metadata
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task<Return<TReturn>> PostInvoke<TParam, TScrapbook, TReturn>(
        Return<TReturn> returned, 
        TScrapbook scrapbook, 
        Metadata<TParam> metadata
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
}

public class RFunc<TParam, TReturn> where TParam : notnull
{
    public RFunc.Invoke<TParam, TReturn> Invoke { get; }
    public RFunc.ReInvoke<TReturn> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    public ScheduleReInvocation ScheduleReInvocation { get; }
    
    public RFunc(
        RFunc.Invoke<TParam, TReturn> invoke, RFunc.ReInvoke<TReturn> reInvoke, 
        Schedule<TParam> schedule, ScheduleReInvocation scheduleReInvocation)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvocation = scheduleReInvocation;
    }
} 
    