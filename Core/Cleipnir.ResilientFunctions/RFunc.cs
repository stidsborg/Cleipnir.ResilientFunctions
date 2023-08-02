using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class RFunc
{
    public delegate Task<TReturn> Invoke<in TParam, in TScrapbook, TReturn>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null)
        where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task Schedule<in TParam, TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();
}

public class RFunc<TParam, TReturn> where TParam : notnull
{
    public RFunc.Invoke<TParam, RScrapbook, TReturn> Invoke { get; }
    public RFunc.Schedule<TParam, RScrapbook> Schedule { get; }
    public ControlPanels<TParam, RScrapbook, TReturn> ControlPanels { get; }
    public EventSourceWriters EventSourceWriters { get; }

    internal RFunc(RFunc<TParam, RScrapbook, TReturn> rFunc)
    {
        Invoke = rFunc.Invoke;
        Schedule = rFunc.Schedule;
        ControlPanels = rFunc.ControlPanel;
        EventSourceWriters = rFunc.EventSourceWriters;
    }
}

public class RFunc<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RFunc.Invoke<TParam, TScrapbook, TReturn> Invoke { get; }
    public RFunc.Schedule<TParam, TScrapbook> Schedule { get; }
    public ControlPanels<TParam, TScrapbook, TReturn> ControlPanel { get; }
    public EventSourceWriters EventSourceWriters { get; }

    internal RFunc(
        RFunc.Invoke<TParam, TScrapbook, TReturn> invoke,
        RFunc.Schedule<TParam, TScrapbook> schedule,
        ControlPanels<TParam, TScrapbook, TReturn> controlPanel, 
        EventSourceWriters eventSourceWriters)
    {
        Invoke = invoke;
        Schedule = schedule;

        ControlPanel = controlPanel;
        EventSourceWriters = eventSourceWriters;
    }
}