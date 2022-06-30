using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class Func
{
    public delegate Task<TReturn> Invoke<in TParam, TReturn>(string functionInstanceId, TParam param)
        where TParam : notnull;

    public delegate Task<TReturn> ReInvoke<TReturn>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null
    );
    
    public delegate Task<TReturn> ReInvoke<TScrapbook, TReturn>(
        string functionInstanceId,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null,
        Action<TScrapbook>? scrapbookUpdater = null
    );
}

public class Func<TParam, TReturn> where TParam : notnull
{
    public Func.Invoke<TParam, TReturn> Invoke { get; }
    public Func.ReInvoke<TReturn> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    public ScheduleReInvocation ScheduleReInvocation { get; }
    
    public Func(
        Func.Invoke<TParam, TReturn> invoke, Func.ReInvoke<TReturn> reInvoke, 
        Schedule<TParam> schedule, ScheduleReInvocation scheduleReInvocation)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvocation = scheduleReInvocation;
    }
} 

public class RFunc<TParam, TScrapbook, TReturn> where TParam : notnull
{
    public Func.Invoke<TParam, TReturn> Invoke { get; }
    public Func.ReInvoke<TScrapbook, TReturn> ReInvoke { get; }
    public Schedule<TParam> Schedule { get; }
    public ScheduleReInvocation<TScrapbook> ScheduleReInvocation { get; }
    
    public RFunc(
        Func.Invoke<TParam, TReturn> invoke, Func.ReInvoke<TScrapbook, TReturn> reInvoke, 
        Schedule<TParam> schedule, ScheduleReInvocation<TScrapbook> scheduleReInvocation)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvocation = scheduleReInvocation;
    }
}
    