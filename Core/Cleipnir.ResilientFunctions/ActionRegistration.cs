using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class ActionRegistration
{
    public delegate Task Invoke<in TParam, in TScrapbook>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();

    public delegate Task Schedule<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param, 
        TScrapbook? scrapbook = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
    
    public delegate Task ScheduleAt<in TParam, TScrapbook>(
        string functionInstanceId, 
        TParam param,
        DateTime delayUntil,
        TScrapbook? scrapbook = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new();
}

public class ActionRegistration<TParam> where TParam : notnull
{
    public FunctionTypeId TypeId { get; }
    
    private readonly RAction<TParam,RScrapbook> _rAction;
    public ActionRegistration.Invoke<TParam, RScrapbook> Invoke { get; }
    public ActionRegistration.Schedule<TParam, RScrapbook> Schedule { get; }
    public ActionRegistration.ScheduleAt<TParam, RScrapbook> ScheduleAt { get; }    
    public MessageWriters MessageWriters { get; }
    
    public ActionRegistration(RAction<TParam, RScrapbook> rAction)
    {
        TypeId = rAction.TypeId;
        
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
    public FunctionTypeId TypeId { get; }
    
    public ActionRegistration.Invoke<TParam, TScrapbook> Invoke { get; }
    public ActionRegistration.Schedule<TParam, TScrapbook> Schedule { get; }
    public ActionRegistration.ScheduleAt<TParam, TScrapbook> ScheduleAt { get; }
    private ControlPanels<TParam, TScrapbook> ControlPanels { get; }
    public MessageWriters MessageWriters { get; }

    public RAction(
        FunctionTypeId functionTypeId,
        ActionRegistration.Invoke<TParam, TScrapbook> invoke,
        ActionRegistration.Schedule<TParam, TScrapbook> schedule,
        ActionRegistration.ScheduleAt<TParam, TScrapbook> scheduleAt,
        ControlPanels<TParam, TScrapbook> controlPanels, 
        MessageWriters messageWriters)
    {
        TypeId = functionTypeId;
        
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