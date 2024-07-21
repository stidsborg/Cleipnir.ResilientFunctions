using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public static class ActionRegistration
{
    public delegate Task Invoke<in TParam>(string flowInstance, TParam param) where TParam : notnull;
    public delegate Task Schedule<in TParam>(string flowInstance, TParam param) where TParam : notnull;
    public delegate Task BulkSchedule<TParam>(IEnumerable<BulkWork<TParam>> instances) where TParam : notnull;

    public delegate Task ScheduleAt<in TParam>(
        string flowInstance,
        TParam param,
        DateTime delayUntil
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
        ActionRegistration.Invoke<TParam> invoke,
        ActionRegistration.Schedule<TParam> schedule,
        ActionRegistration.ScheduleAt<TParam> scheduleAt,
        ActionRegistration.BulkSchedule<TParam> bulkSchedule,
        ControlPanelFactory<TParam> controlPanelFactory, 
        MessageWriters messageWriters, 
        StateFetcher stateFetcher,
        Postman postman
    ) : base(postman)
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
        where TState : FlowState, new()
    {
        var functionId = new FlowId(Type, instance);
        return stateId is null 
            ? _stateFetcher.FetchState<TState>(functionId) 
            : _stateFetcher.FetchState<TState>(functionId, stateId);
    }
    
    public Task ScheduleIn(string flowInstance, TParam param, TimeSpan delay) 
        => ScheduleAt(flowInstance, param, delayUntil: DateTime.UtcNow.Add(delay));
}