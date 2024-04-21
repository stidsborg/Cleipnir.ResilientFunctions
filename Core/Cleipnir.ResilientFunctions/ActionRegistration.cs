using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class ActionRegistration
{
    public delegate Task Invoke<in TParam>(string functionInstanceId, TParam param) where TParam : notnull;
    public delegate Task Schedule<in TParam>(string functionInstanceId, TParam param) where TParam : notnull;

    public delegate Task ScheduleAt<in TParam>(
        string functionInstanceId,
        TParam param,
        DateTime delayUntil
    ) where TParam : notnull;
}

public class ActionRegistration<TParam> where TParam : notnull
{
    private readonly ControlPanels<TParam> _controlPanels;
    public FunctionTypeId TypeId { get; }
    
    public ActionRegistration.Invoke<TParam> Invoke { get; }
    public ActionRegistration.Schedule<TParam> Schedule { get; }
    public ActionRegistration.ScheduleAt<TParam> ScheduleAt { get; }   
    private readonly StateFetcher _stateFetcher;
    public MessageWriters MessageWriters { get; }
    
    public ActionRegistration(
        FunctionTypeId functionTypeId,
        ActionRegistration.Invoke<TParam> invoke,
        ActionRegistration.Schedule<TParam> schedule,
        ActionRegistration.ScheduleAt<TParam> scheduleAt,
        ControlPanels<TParam> controlPanels, 
        MessageWriters messageWriters, 
        StateFetcher stateFetcher)
    {
        TypeId = functionTypeId;
        
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;
        _controlPanels = controlPanels;
        MessageWriters = messageWriters;
        _stateFetcher = stateFetcher;
    }

    public Task<ControlPanel<TParam>?> ControlPanel(FunctionInstanceId functionInstanceId)
        => _controlPanels.For(functionInstanceId);

    public Task<TState?> GetState<TState>(FunctionInstanceId instanceId, StateId? stateId = null)
        where TState : WorkflowState, new()
    {
        var functionId = new FunctionId(TypeId, instanceId);
        return stateId is null 
            ? _stateFetcher.FetchState<TState>(functionId) 
            : _stateFetcher.FetchState<TState>(functionId, stateId);
    }
         
    
    public Task ScheduleIn(string functionInstanceId, TParam param, TimeSpan delay) 
        => ScheduleAt(functionInstanceId, param, delayUntil: DateTime.UtcNow.Add(delay));
}