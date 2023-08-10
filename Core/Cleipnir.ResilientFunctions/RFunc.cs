using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class RFunc
{
    public delegate Task<TReturn> Invoke<in TParam, in TScrapbook, TReturn>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null, 
        IEnumerable<EventAndIdempotencyKey>? events = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task Schedule<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null,
        IEnumerable<EventAndIdempotencyKey>? events = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task ScheduleAt<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param,
        DateTime delayUntil,
        TScrapbook? scrapbook = null,
        IEnumerable<EventAndIdempotencyKey>? events = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
}

public class RFunc<TParam, TReturn> where TParam : notnull
{
    public RFunc.Invoke<TParam, RScrapbook, TReturn> Invoke { get; }
    public RFunc.Schedule<TParam, RScrapbook> Schedule { get; }
    public RFunc.ScheduleAt<TParam, RScrapbook> ScheduleAt { get; }
    public ControlPanels<TParam, RScrapbook, TReturn> ControlPanels { get; }
    public EventSourceWriters EventSourceWriters { get; }

    public RFunc(RFunc<TParam, RScrapbook, TReturn> rFunc)
    {
        Invoke = rFunc.Invoke;
        Schedule = rFunc.Schedule;
        ScheduleAt = rFunc.ScheduleAt;
        ControlPanels = rFunc.ControlPanel;
        EventSourceWriters = rFunc.EventSourceWriters;
    }
    
    public Task ScheduleIn(
        string functionInstanceId,
        TParam param,
        TimeSpan delay,
        RScrapbook? scrapbook = null,
        IEnumerable<EventAndIdempotencyKey>? events = null
    ) => ScheduleAt(
        functionInstanceId,
        param,
        delayUntil: DateTime.UtcNow.Add(delay),
        scrapbook,
        events
    );
}

public class RFunc<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RFunc.Invoke<TParam, TScrapbook, TReturn> Invoke { get; }
    public RFunc.Schedule<TParam, TScrapbook> Schedule { get; }
    public RFunc.ScheduleAt<TParam, TScrapbook> ScheduleAt { get; }
    public ControlPanels<TParam, TScrapbook, TReturn> ControlPanel { get; }
    public EventSourceWriters EventSourceWriters { get; }

    public RFunc(
        RFunc.Invoke<TParam, TScrapbook, TReturn> invoke,
        RFunc.Schedule<TParam, TScrapbook> schedule,
        RFunc.ScheduleAt<TParam, TScrapbook> scheduleAt,
        ControlPanels<TParam, TScrapbook, TReturn> controlPanel, 
        EventSourceWriters eventSourceWriters)
    {
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;

        ControlPanel = controlPanel;
        EventSourceWriters = eventSourceWriters;
    }

    public Task ScheduleIn(
        string functionInstanceId,
        TParam param,
        TimeSpan delay,
        TScrapbook? scrapbook = null,
        IEnumerable<EventAndIdempotencyKey>? events = null
    ) => ScheduleAt(
        functionInstanceId,
        param,
        delayUntil: DateTime.UtcNow.Add(delay),
        scrapbook,
        events
    );
}