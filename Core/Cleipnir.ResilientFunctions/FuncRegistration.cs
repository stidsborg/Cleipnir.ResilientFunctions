using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public static class FuncRegistration
{
    public delegate Task<TReturn> Invoke<in TParam, TReturn>(
        FlowInstance flowInstance,
        TParam param
    ) where TParam : notnull;

    public delegate Task Schedule<in TParam>(
        FlowInstance flowInstance,
        TParam param
    ) where TParam : notnull;

    public delegate Task ScheduleAt<in TParam>(
        FlowInstance flowInstance,
        TParam param,
        DateTime delayUntil
    ) where TParam : notnull;
    
    public delegate Task BulkSchedule<TParam>(IEnumerable<BulkWork<TParam>> instances) where TParam : notnull;
}

public class FuncRegistration<TParam, TReturn> : BaseRegistration where TParam : notnull
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
        StoredType storedType,
        FuncRegistration.Invoke<TParam, TReturn> invoke,
        FuncRegistration.Schedule<TParam> schedule,
        FuncRegistration.ScheduleAt<TParam> scheduleAt,
        FuncRegistration.BulkSchedule<TParam> bulkSchedule,
        GetInstances getInstances,
        ControlPanelFactory<TParam, TReturn> controlPanelFactory, 
        MessageWriters messageWriters, 
        StateFetcher stateFetcher,
        Postman postman
    ) : base(storedType, postman, getInstances)
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
        return stateId is null 
            ? _stateFetcher.FetchState<TState>(instance) 
            : _stateFetcher.FetchState<TState>(instance, stateId);
    }

    public Task ScheduleIn(string flowInstance, TParam param, TimeSpan delay) 
        => ScheduleAt(flowInstance, param, delayUntil: DateTime.UtcNow.Add(delay));
    
    public async Task<Finding> SendMessage<T>(
        FlowInstance flowInstance,
        T message,
        string? idempotencyKey = null
    ) where T : notnull => await Postman.SendMessage(flowInstance.Value.ToStoredInstance(), message, idempotencyKey);
}