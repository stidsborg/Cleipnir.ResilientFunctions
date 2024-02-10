using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class ActionRegistration
{
    public delegate Task Invoke<in TParam, in TState>(
        string functionInstanceId, 
        TParam param, 
        TState? state = null
    ) where TParam : notnull where TState : WorkflowState, new();

    public delegate Task Schedule<in TParam, TState>(
        string functionInstanceId, 
        TParam param, 
        TState? state = null
    ) where TParam : notnull where TState : WorkflowState, new();
    
    public delegate Task ScheduleAt<in TParam, TState>(
        string functionInstanceId, 
        TParam param,
        DateTime delayUntil,
        TState? state = null
    ) where TParam : notnull where TState : WorkflowState, new();
}

public class ActionRegistration<TParam> where TParam : notnull
{
    public FunctionTypeId TypeId { get; }
    
    private readonly ActionRegistration<TParam,WorkflowState> _actionRegistration;
    public ActionRegistration.Invoke<TParam, WorkflowState> Invoke { get; }
    public ActionRegistration.Schedule<TParam, WorkflowState> Schedule { get; }
    public ActionRegistration.ScheduleAt<TParam, WorkflowState> ScheduleAt { get; }    
    public MessageWriters MessageWriters { get; }
    
    public ActionRegistration(ActionRegistration<TParam, WorkflowState> actionRegistration)
    {
        TypeId = actionRegistration.TypeId;
        
        _actionRegistration = actionRegistration;
        Invoke = actionRegistration.Invoke;
        Schedule = actionRegistration.Schedule;
        ScheduleAt = actionRegistration.ScheduleAt;
        
        MessageWriters = actionRegistration.MessageWriters;
    }

    public Task<ControlPanel<TParam, WorkflowState>?> ControlPanel(FunctionInstanceId functionInstanceId)
        => _actionRegistration.ControlPanel(functionInstanceId);
    
    public Task ScheduleIn(
        string functionInstanceId,
        TParam param,
        TimeSpan delay,
        WorkflowState? state = null
    ) => ScheduleAt(
        functionInstanceId,
        param,
        delayUntil: DateTime.UtcNow.Add(delay),
        state
    );
}

public class ActionRegistration<TParam, TState> where TParam : notnull where TState : WorkflowState, new()
{
    public FunctionTypeId TypeId { get; }
    
    public ActionRegistration.Invoke<TParam, TState> Invoke { get; }
    public ActionRegistration.Schedule<TParam, TState> Schedule { get; }
    public ActionRegistration.ScheduleAt<TParam, TState> ScheduleAt { get; }
    private ControlPanels<TParam, TState> ControlPanels { get; }
    public MessageWriters MessageWriters { get; }

    public ActionRegistration(
        FunctionTypeId functionTypeId,
        ActionRegistration.Invoke<TParam, TState> invoke,
        ActionRegistration.Schedule<TParam, TState> schedule,
        ActionRegistration.ScheduleAt<TParam, TState> scheduleAt,
        ControlPanels<TParam, TState> controlPanels, 
        MessageWriters messageWriters)
    {
        TypeId = functionTypeId;
        
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;
        ControlPanels = controlPanels;
        MessageWriters = messageWriters;
    }
    
    public Task<ControlPanel<TParam, TState>?> ControlPanel(FunctionInstanceId functionInstanceId)
        => ControlPanels.For(functionInstanceId);
    
    public Task ScheduleIn(
        string functionInstanceId,
        TParam param,
        TimeSpan delay,
        TState? state = null
    ) => ScheduleAt(
        functionInstanceId,
        param,
        delayUntil: DateTime.UtcNow.Add(delay),
        state
    );
}