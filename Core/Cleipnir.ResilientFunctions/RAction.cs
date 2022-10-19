﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task Invoke<in TParam, in TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task ReInvoke(string functionInstanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch = null);
    
    public delegate Task Schedule<in TParam, TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task ScheduleReInvoke(string functionInstanceId, IEnumerable<Status> expectedStatuses, int? expectedEpoch = null);
}

public class RAction<TParam> where TParam : notnull
{
    public RAction.Invoke<TParam, RScrapbook> Invoke { get; }
    public RAction.ReInvoke ReInvoke { get; }
    public RAction.Schedule<TParam, RScrapbook> Schedule { get; }
    public RAction.ScheduleReInvoke ScheduleReInvoke { get; }
    public RAdmin<TParam, RScrapbook, Unit> Admin { get; }
    
    public RAction(RAction<TParam, RScrapbook> rAction)
    {
        Invoke = rAction.Invoke;
        ReInvoke = rAction.ReInvoke;
        Schedule = rAction.Schedule;
        ScheduleReInvoke = rAction.ScheduleReInvoke;

        Admin = rAction.Admin;
    }
}

public class RAction<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RAction.Invoke<TParam, TScrapbook> Invoke { get; }
    public RAction.ReInvoke ReInvoke { get; }
    public RAction.Schedule<TParam, TScrapbook> Schedule { get; }
    public RAction.ScheduleReInvoke ScheduleReInvoke { get; }
    public RAdmin<TParam, TScrapbook, Unit> Admin { get; }

    internal RAction(
        FunctionTypeId functionTypeId,
        InvocationHelper<TParam, TScrapbook, Unit> invocationHelper,
        RAction.Invoke<TParam, TScrapbook> invoke, 
        RAction.ReInvoke reInvoke, 
        RAction.Schedule<TParam, TScrapbook> schedule, 
        RAction.ScheduleReInvoke scheduleReInvoke
    )
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvoke = scheduleReInvoke;
        Admin = new RAdmin<TParam, TScrapbook, Unit>(functionTypeId, invocationHelper);
    }
}