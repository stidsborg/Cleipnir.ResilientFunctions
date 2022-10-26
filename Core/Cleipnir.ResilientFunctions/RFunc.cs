using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public static class RFunc
{
    public delegate Task<TReturn> Invoke<in TParam, in TScrapbook, TReturn>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null)
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task<TReturn> ReInvoke<TReturn>(string functionInstanceId, int expectedEpoch);
    
    public delegate Task Schedule<in TParam, TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
        where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task ScheduleReInvoke(string functionInstanceId, int expectedEpoch);
}

public class RFunc<TParam, TReturn> where TParam : notnull
{
    public RFunc.Invoke<TParam, RScrapbook, TReturn> Invoke { get; }
    public RFunc.ReInvoke<TReturn> ReInvoke { get; }
    public RFunc.Schedule<TParam, RScrapbook> Schedule { get; }
    public RFunc.ScheduleReInvoke ScheduleReInvoke { get; }
    public ControlPanelFactory<TParam, RScrapbook, TReturn> ControlPanel { get; }

    internal RFunc(RFunc<TParam, RScrapbook, TReturn> rFunc)
    {
        Invoke = rFunc.Invoke;
        ReInvoke = rFunc.ReInvoke;
        Schedule = rFunc.Schedule;
        ScheduleReInvoke = rFunc.ScheduleReInvoke;
        ControlPanel = rFunc.ControlPanel;
    }
}

public class RFunc<TParam, TScrapbook, TReturn> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RFunc.Invoke<TParam, TScrapbook, TReturn> Invoke { get; }
    public RFunc.ReInvoke<TReturn> ReInvoke { get; }
    public RFunc.Schedule<TParam, TScrapbook> Schedule { get; }
    public RFunc.ScheduleReInvoke ScheduleReInvoke { get; }
    public ControlPanelFactory<TParam, TScrapbook, TReturn> ControlPanel { get; }

    internal RFunc(
        RFunc.Invoke<TParam, TScrapbook, TReturn> invoke, 
        RFunc.ReInvoke<TReturn> reInvoke, 
        RFunc.Schedule<TParam, TScrapbook> schedule, 
        RFunc.ScheduleReInvoke scheduleReInvoke,
        ControlPanelFactory<TParam, TScrapbook, TReturn> controlPanel)
    {
        Invoke = invoke;
        ReInvoke = reInvoke;
        Schedule = schedule;
        ScheduleReInvoke = scheduleReInvoke;

        ControlPanel = controlPanel;
    }
}