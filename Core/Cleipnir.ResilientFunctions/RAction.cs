using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task Invoke<in TParam, in TScrapbook>(
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

public class RAction<TParam> where TParam : notnull
{
    public RAction.Invoke<TParam, RScrapbook> Invoke { get; }
    public RAction.Schedule<TParam, RScrapbook> Schedule { get; }
    public RAction.ScheduleAt<TParam, RScrapbook> ScheduleAt { get; }    
    public ControlPanels<TParam, RScrapbook> ControlPanels { get; }
    public EventSourceWriters EventSourceWriters { get; }
    
    public RAction(RAction<TParam, RScrapbook> rAction)
    {
        Invoke = rAction.Invoke;
        Schedule = rAction.Schedule;
        ScheduleAt = rAction.ScheduleAt;

        ControlPanels = rAction.ControlPanels;
        EventSourceWriters = rAction.EventSourceWriters;
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

public class RAction<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RAction.Invoke<TParam, TScrapbook> Invoke { get; }
    public RAction.Schedule<TParam, TScrapbook> Schedule { get; }
    public RAction.ScheduleAt<TParam, TScrapbook> ScheduleAt { get; }
    public ControlPanels<TParam, TScrapbook> ControlPanels { get; }
    public EventSourceWriters EventSourceWriters { get; }

    public RAction(
        RAction.Invoke<TParam, TScrapbook> invoke,
        RAction.Schedule<TParam, TScrapbook> schedule,
        RAction.ScheduleAt<TParam, TScrapbook> scheduleAt,
        ControlPanels<TParam, TScrapbook> controlPanels, 
        EventSourceWriters eventSourceWriters)
    {
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;
        ControlPanels = controlPanels;
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