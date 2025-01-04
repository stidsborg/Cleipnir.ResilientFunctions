using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public static class ActionRegistration
{
    public delegate Task Invoke<in TParam>(FlowInstance flowInstance, TParam param) where TParam : notnull;
    public delegate Task<Scheduled> Schedule<in TParam>(FlowInstance flowInstance, TParam param, bool? detach = null) where TParam : notnull;
    public delegate Task<BulkScheduled> BulkSchedule<TParam>(IEnumerable<BulkWork<TParam>> instances, bool? detach = null) where TParam : notnull;

    public delegate Task<Scheduled> ScheduleAt<in TParam>(
        FlowInstance flowInstance,
        TParam param,
        DateTime delayUntil,
        bool? detach = null
    ) where TParam : notnull;
}

public class ActionRegistration<TParam> : BaseRegistration where TParam : notnull
{
    private readonly ControlPanelFactory<TParam> _controlPanelFactory;
    public FlowType Type { get; }
    
    public ActionRegistration.Invoke<TParam> Invoke { get; }
    public ActionRegistration.Schedule<TParam> Schedule { get; }
    public ActionRegistration.ScheduleAt<TParam> ScheduleAt { get; }
    public ActionRegistration.BulkSchedule<TParam> BulkSchedule { get; }
    
    private readonly StateFetcher _stateFetcher;
    public MessageWriters MessageWriters { get; }
    
    public ActionRegistration(
        FlowType flowType,
        StoredType storedType,
        IFunctionStore functionStore,
        ActionRegistration.Invoke<TParam> invoke,
        ActionRegistration.Schedule<TParam> schedule,
        ActionRegistration.ScheduleAt<TParam> scheduleAt,
        ActionRegistration.BulkSchedule<TParam> bulkSchedule,
        ControlPanelFactory<TParam> controlPanelFactory, 
        MessageWriters messageWriters, 
        StateFetcher stateFetcher,
        Postman postman
    ) : base(storedType, postman, functionStore)
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

    public Task<ControlPanel<TParam>?> ControlPanel(FlowInstance flowInstance)
        => _controlPanelFactory.Create(flowInstance);

    public Task<TState?> GetState<TState>(FlowInstance instance, StateId? stateId = null)
        where TState : FlowState, new() => stateId is null
        ? _stateFetcher.FetchState<TState>(instance)
        : _stateFetcher.FetchState<TState>(instance, stateId);
    
    public Task ScheduleIn(string flowInstance, TParam param, TimeSpan delay, bool? detach = null) 
        => ScheduleAt(flowInstance, param, delayUntil: DateTime.UtcNow.Add(delay), detach);
    
    public async Task<Finding> SendMessage<T>(
        FlowInstance flowInstance,
        T message,
        string? idempotencyKey = null
    ) where T : notnull => await Postman.SendMessage(flowInstance.Value.ToStoredInstance(), message, idempotencyKey);
}