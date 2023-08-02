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
}

public class RAction<TParam> where TParam : notnull
{
    public RAction.Invoke<TParam, RScrapbook> Invoke { get; }
    public RAction.Schedule<TParam, RScrapbook> Schedule { get; }
    public ControlPanels<TParam, RScrapbook> ControlPanels { get; }
    public EventSourceWriters EventSourceWriters { get; }
    
    public RAction(RAction<TParam, RScrapbook> rAction)
    {
        Invoke = rAction.Invoke;
        Schedule = rAction.Schedule;

        ControlPanels = rAction.ControlPanels;
        EventSourceWriters = rAction.EventSourceWriters;
    }
}

public class RAction<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RAction.Invoke<TParam, TScrapbook> Invoke { get; }
    public RAction.Schedule<TParam, TScrapbook> Schedule { get; }
    public ControlPanels<TParam, TScrapbook> ControlPanels { get; }
    public EventSourceWriters EventSourceWriters { get; }

    internal RAction(
        RAction.Invoke<TParam, TScrapbook> invoke,
        RAction.Schedule<TParam, TScrapbook> schedule,
        ControlPanels<TParam, TScrapbook> controlPanels, 
        EventSourceWriters eventSourceWriters)
    {
        Invoke = invoke;
        Schedule = schedule;
        ControlPanels = controlPanels;
        EventSourceWriters = eventSourceWriters;
    }
}