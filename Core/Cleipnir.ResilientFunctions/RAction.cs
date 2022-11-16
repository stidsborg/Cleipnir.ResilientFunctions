using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task Invoke<in TParam, in TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task ReInvoke(string functionInstanceId, int expectedEpoch);
    
    public delegate Task Schedule<in TParam, TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task ScheduleReInvoke(string functionInstanceId, int expectedEpoch);
}

public class RAction<TParam> where TParam : notnull
{
    public RAction.Invoke<TParam, RScrapbook> Invoke { get; }
    public RAction.Schedule<TParam, RScrapbook> Schedule { get; }
    public ControlPanelFactory<TParam, RScrapbook> ControlPanel { get; }
    public EventSourceWriters EventSourceWriters { get; }
    
    public RAction(RAction<TParam, RScrapbook> rAction)
    {
        Invoke = rAction.Invoke;
        Schedule = rAction.Schedule;

        ControlPanel = rAction.ControlPanel;
        EventSourceWriters = rAction.EventSourceWriters;
    }
}

public class RAction<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RAction.Invoke<TParam, TScrapbook> Invoke { get; }
    public RAction.Schedule<TParam, TScrapbook> Schedule { get; }
    public ControlPanelFactory<TParam, TScrapbook> ControlPanel { get; }
    public EventSourceWriters EventSourceWriters { get; }

    internal RAction(
        RAction.Invoke<TParam, TScrapbook> invoke,
        RAction.Schedule<TParam, TScrapbook> schedule,
        ControlPanelFactory<TParam, TScrapbook> controlPanelFactory, 
        EventSourceWriters eventSourceWriters)
    {
        Invoke = invoke;
        Schedule = schedule;
        ControlPanel = controlPanelFactory;
        EventSourceWriters = eventSourceWriters;
    }
}