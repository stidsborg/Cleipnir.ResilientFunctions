using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class Action
{
    public delegate Task Invoke<in TParam>(string functionInstanceId, TParam param) where TParam : notnull;

    public delegate Task ReInvoke(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null
    );
    
    public delegate Task ReInvoke<TScrapbook>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null,
        Action<TScrapbook>? scrapbookUpdater = null
    );
}

public class RAction<TParam> where TParam : notnull
{
    public Action.Invoke<TParam> Invoke { get; }
    public Action.ReInvoke ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    public ScheduleReInvocation ScheduleReInvocation { get; }

    public RAction(
        Action.Invoke<TParam> invoke, Action.ReInvoke reInvoke, 
        Schedule<TParam> schedule, ScheduleReInvocation scheduleReInvocation)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvocation = scheduleReInvocation;
    }
} 

public class RAction<TParam, TScrapbook> where TParam : notnull
{
    public Action.Invoke<TParam> Invoke { get; }
    public Action.ReInvoke<TScrapbook> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    public ScheduleReInvocation<TScrapbook> ScheduleReInvocation { get; }

    public RAction(
        Action.Invoke<TParam> invoke, Action.ReInvoke<TScrapbook> reInvoke, 
        Schedule<TParam> schedule, ScheduleReInvocation<TScrapbook> scheduleReInvocation)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvocation = scheduleReInvocation;
    }
} 