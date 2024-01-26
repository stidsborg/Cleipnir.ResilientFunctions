using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class RAction
{
    public delegate Task Invoke<in TParam, in TScrapbook>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task Schedule<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null,
        FunctionId? sendResultTo = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task ScheduleAt<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param,
        DateTime delayUntil,
        TScrapbook? scrapbook = null,
        FunctionId? sendResultTo = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
}

public class RAction<TParam> where TParam : notnull
{
    private readonly RAction<TParam,RScrapbook> _rAction;
    public RAction.Invoke<TParam, RScrapbook> Invoke { get; }
    public RAction.Schedule<TParam, RScrapbook> Schedule { get; }
    public RAction.ScheduleAt<TParam, RScrapbook> ScheduleAt { get; }    
    public MessageWriters MessageWriters { get; }
    
    public RAction(RAction<TParam, RScrapbook> rAction)
    {
        _rAction = rAction;
        Invoke = rAction.Invoke;
        Schedule = rAction.Schedule;
        ScheduleAt = rAction.ScheduleAt;
        
        MessageWriters = rAction.MessageWriters;
    }

    public Task<ControlPanel<TParam, RScrapbook>?> ControlPanel(FunctionInstanceId functionInstanceId)
        => _rAction.ControlPanel(functionInstanceId);
    
    public Task ScheduleIn(
        string functionInstanceId,
        TParam param,
        TimeSpan delay,
        RScrapbook? scrapbook = null
    ) => ScheduleAt(
        functionInstanceId,
        param,
        delayUntil: DateTime.UtcNow.Add(delay),
        scrapbook
    );
}

public class RAction<TParam, TScrapbook> where TParam : notnull where TScrapbook : RScrapbook, new()
{
    public RAction.Invoke<TParam, TScrapbook> Invoke { get; }
    public RAction.Schedule<TParam, TScrapbook> Schedule { get; }
    public RAction.ScheduleAt<TParam, TScrapbook> ScheduleAt { get; }
    private ControlPanels<TParam, TScrapbook> ControlPanels { get; }
    public MessageWriters MessageWriters { get; }

    public RAction(
        RAction.Invoke<TParam, TScrapbook> invoke,
        RAction.Schedule<TParam, TScrapbook> schedule,
        RAction.ScheduleAt<TParam, TScrapbook> scheduleAt,
        ControlPanels<TParam, TScrapbook> controlPanels, 
        MessageWriters messageWriters)
    {
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;
        ControlPanels = controlPanels;
        MessageWriters = messageWriters;
    }
    
    public Task<ControlPanel<TParam, TScrapbook>?> ControlPanel(FunctionInstanceId functionInstanceId)
        => ControlPanels.For(functionInstanceId);
    
    public Task ScheduleIn(
        string functionInstanceId,
        TParam param,
        TimeSpan delay,
        TScrapbook? scrapbook = null
    ) => ScheduleAt(
        functionInstanceId,
        param,
        delayUntil: DateTime.UtcNow.Add(delay),
        scrapbook
    );
}