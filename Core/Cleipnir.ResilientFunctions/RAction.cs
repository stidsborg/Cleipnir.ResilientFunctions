using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

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
    public RAction.ReInvoke ReInvoke { get; }
    public RAction.Schedule<TParam, RScrapbook> Schedule { get; }
    public RAction.ScheduleReInvoke ScheduleReInvoke { get; }
    public ControlPanelFactory<TParam, RScrapbook> ControlPanel { get; }
    
    public RAction(RAction<TParam, RScrapbook> rAction)
    {
        Invoke = rAction.Invoke;
        ReInvoke = rAction.ReInvoke;
        Schedule = rAction.Schedule;
        ScheduleReInvoke = rAction.ScheduleReInvoke;

        ControlPanel = rAction.ControlPanel;
    }
}

public class RAction<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RAction.Invoke<TParam, TScrapbook> Invoke { get; }
    public RAction.ReInvoke ReInvoke { get; }
    public RAction.Schedule<TParam, TScrapbook> Schedule { get; }
    public RAction.ScheduleReInvoke ScheduleReInvoke { get; }
    public ControlPanelFactory<TParam, TScrapbook> ControlPanel { get; }

    internal RAction(
        RAction.Invoke<TParam, TScrapbook> invoke, 
        RAction.ReInvoke reInvoke, 
        RAction.Schedule<TParam, TScrapbook> schedule, 
        RAction.ScheduleReInvoke scheduleReInvoke,
        ControlPanelFactory<TParam, TScrapbook> controlPanelFactory)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvoke = scheduleReInvoke;
        ControlPanel = controlPanelFactory;
    }
}