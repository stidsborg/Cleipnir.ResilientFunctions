using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task Invoke<in TParam, in TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task ReInvoke<TScrapbook>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null,
        Action<TScrapbook>? scrapbookUpdater = null
    );
    
    public delegate Task Schedule<in TParam, TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task ScheduleReInvoke<TScrapbook>(
        string functionInstanceId, 
        IEnumerable<Status> expectedStatuses, 
        int? expectedEpoch = null,
        Action<TScrapbook>? scrapbookUpdater = null
    );
}

public class RAction<TParam> where TParam : notnull
{
    public RAction.Invoke<TParam, RScrapbook> Invoke { get; }
    public RAction.ReInvoke<RScrapbook> ReInvoke { get; }
    public RAction.Schedule<TParam, RScrapbook> Schedule { get; }
    public RAction.ScheduleReInvoke<RScrapbook> ScheduleReInvoke { get; }

    public RAction(RAction.Invoke<TParam, RScrapbook> invoke, RAction.ReInvoke<RScrapbook> reInvoke, RAction.Schedule<TParam, RScrapbook> schedule, RAction.ScheduleReInvoke<RScrapbook> scheduleReInvoke)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvoke = scheduleReInvoke;
    }
}

public class RAction<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RAction.Invoke<TParam, TScrapbook> Invoke { get; }
    public RAction.ReInvoke<TScrapbook> ReInvoke { get; }
    public RAction.Schedule<TParam, TScrapbook> Schedule { get; }
    public RAction.ScheduleReInvoke<TScrapbook> ScheduleReInvoke { get; }

    public RAction(RAction.Invoke<TParam, TScrapbook> invoke, RAction.ReInvoke<TScrapbook> reInvoke, RAction.Schedule<TParam, TScrapbook> schedule, RAction.ScheduleReInvoke<TScrapbook> scheduleReInvoke)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvoke = scheduleReInvoke;
    }
}

public static class RActionExtensions
{
    public static RAction<TParam> ConvertToRActionWithoutScrapbook<TParam>(this RAction<TParam, RScrapbook> rAction) 
        where TParam : notnull 
        => new(rAction.Invoke, rAction.ReInvoke, rAction.Schedule, rAction.ScheduleReInvoke);
} 