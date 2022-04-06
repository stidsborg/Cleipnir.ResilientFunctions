using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task Invoke<in TParam>(string functionInstanceId, TParam param) where TParam : notnull;

    public delegate Task ReInvoke(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null
    );

    public delegate Return SyncPostInvoke<TParam, TScrapbook>(Return returned, TScrapbook scrapbook, Metadata<TParam> metadata)
        where TParam : notnull where TScrapbook : RScrapbook, new();
}

public class RAction<TParam> where TParam : notnull
{
    public RAction.Invoke<TParam> Invoke { get; }
    public RAction.ReInvoke ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    public ScheduleReInvocation ScheduleReInvocation { get; }

    public RAction(
        RAction.Invoke<TParam> invoke, RAction.ReInvoke reInvoke, 
        Schedule<TParam> schedule, ScheduleReInvocation scheduleReInvocation)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvocation = scheduleReInvocation;
    }
} 