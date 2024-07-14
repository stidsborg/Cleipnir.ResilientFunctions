using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class FuncRegistration
{
    public delegate Task<TReturn> Invoke<in TParam, TReturn>(
        string functionInstanceId,
        TParam param
    ) where TParam : notnull;

    public delegate Task Schedule<in TParam>(
        string functionInstanceId,
        TParam param
    ) where TParam : notnull;

    public delegate Task ScheduleAt<in TParam>(
        string functionInstanceId,
        TParam param,
        DateTime delayUntil
    ) where TParam : notnull;
    
    public delegate Task BulkSchedule<TParam>(IEnumerable<BulkWork<TParam>> instances) where TParam : notnull;
}

public class FuncRegistration<TParam, TReturn> where TParam : notnull
{
    public FlowType Type { get; }
    
    public FuncRegistration.Invoke<TParam, TReturn> Invoke { get; }
    public FuncRegistration.Schedule<TParam> Schedule { get; }
    public FuncRegistration.ScheduleAt<TParam> ScheduleAt { get; }
    public FuncRegistration.BulkSchedule<TParam> BulkSchedule { get; } 
    
    private readonly ControlPanelFactory<TParam,TReturn> _controlPanelFactory;
    private readonly StateFetcher _stateFetcher;
    public MessageWriters MessageWriters { get; }

    public FuncRegistration(
        FlowType flowType,
        FuncRegistration.Invoke<TParam, TReturn> invoke,
        FuncRegistration.Schedule<TParam> schedule,
        FuncRegistration.ScheduleAt<TParam> scheduleAt,
        FuncRegistration.BulkSchedule<TParam> bulkSchedule,
        ControlPanelFactory<TParam, TReturn> controlPanelFactory, 
        MessageWriters messageWriters, 
        StateFetcher stateFetcher)
    {
        Type = flowType;
        
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;
        BulkSchedule = bulkSchedule;

        _controlPanelFactory = controlPanelFactory;
        MessageWriters = messageWriters;
        _stateFetcher = stateFetcher;
    }

    public Task<ControlPanel<TParam, TReturn>?> ControlPanel(FlowInstance flowInstance)
        => _controlPanelFactory.Create(flowInstance);

    public Task<TState?> GetState<TState>(FlowInstance instance, StateId? stateId = null) 
        where TState : FlowState, new()
    {
        var functionId = new FlowId(Type, instance);
        return stateId is null 
            ? _stateFetcher.FetchState<TState>(functionId) 
            : _stateFetcher.FetchState<TState>(functionId, stateId);
    }

    public Task ScheduleIn(string functionInstanceId, TParam param, TimeSpan delay) 
        => ScheduleAt(functionInstanceId, param, delayUntil: DateTime.UtcNow.Add(delay));
}