using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class FuncRegistration
{
    public delegate Task<TReturn> Invoke<in TParam, in TState, TReturn>(
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

public class FuncRegistration<TParam, TReturn> where TParam : notnull
{
    public FunctionTypeId TypeId { get; }
    
    public FuncRegistration.Invoke<TParam, WorkflowState, TReturn> Invoke { get; }
    public FuncRegistration.Schedule<TParam, WorkflowState> Schedule { get; }
    public FuncRegistration.ScheduleAt<TParam, WorkflowState> ScheduleAt { get; }
    private readonly FuncRegistration<TParam, WorkflowState, TReturn> _funcRegistration; 
    public MessageWriters MessageWriters { get; }

    public FuncRegistration(FuncRegistration<TParam, WorkflowState, TReturn> funcRegistration)
    {
        TypeId = funcRegistration.TypeId;
        
        _funcRegistration = funcRegistration;
        
        Invoke = funcRegistration.Invoke;
        Schedule = funcRegistration.Schedule;
        ScheduleAt = funcRegistration.ScheduleAt;
        MessageWriters = funcRegistration.MessageWriters;
    }

    public Task<ControlPanel<TParam, WorkflowState, TReturn>?> ControlPanel(FunctionInstanceId functionInstanceId)
        => _funcRegistration.ControlPanel(functionInstanceId);
    
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

public class FuncRegistration<TParam, TState, TReturn> where TParam : notnull where TState : WorkflowState, new()
{
    public FunctionTypeId TypeId { get; }
    
    public FuncRegistration.Invoke<TParam, TState, TReturn> Invoke { get; }
    public FuncRegistration.Schedule<TParam, TState> Schedule { get; }
    public FuncRegistration.ScheduleAt<TParam, TState> ScheduleAt { get; }
    private ControlPanels<TParam, TState, TReturn> ControlPanels { get; }
    public MessageWriters MessageWriters { get; }

    public FuncRegistration(
        FunctionTypeId functionTypeId,
        FuncRegistration.Invoke<TParam, TState, TReturn> invoke,
        FuncRegistration.Schedule<TParam, TState> schedule,
        FuncRegistration.ScheduleAt<TParam, TState> scheduleAt,
        ControlPanels<TParam, TState, TReturn> controlPanel, 
        MessageWriters messageWriters)
    {
        TypeId = functionTypeId;
        
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;

        ControlPanels = controlPanel;
        MessageWriters = messageWriters;
    }

    public Task<ControlPanel<TParam, TState, TReturn>?> ControlPanel(FunctionInstanceId functionInstanceId)
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