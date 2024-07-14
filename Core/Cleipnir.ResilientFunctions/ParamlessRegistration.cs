﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public class ParamlessRegistration
{
    private readonly ControlPanelFactory _controlPanelFactory;
    public FlowType Type { get; }
    
    public Func<FlowInstance, Task> Invoke { get; }
    public Func<FlowInstance, Task> Schedule { get; }
    public Func<FlowInstance, DateTime, Task> ScheduleAt { get; }
    public Func<IEnumerable<FlowInstance>, Task> BulkSchedule { get; }
    
    private readonly StateFetcher _stateFetcher;
    public MessageWriters MessageWriters { get; }
    
    public ParamlessRegistration(
        FlowType flowType,
        Func<FlowInstance, Task> invoke,
        Func<FlowInstance, Task> schedule,
        Func<FlowInstance, DateTime, Task> scheduleAt,
        Func<IEnumerable<FlowInstance>, Task> bulkSchedule,
        ControlPanelFactory controlPanelFactory, 
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

    public Task<ControlPanel?> ControlPanel(FlowInstance flowInstance)
        => _controlPanelFactory.Create(flowInstance);

    public Task<TState?> GetState<TState>(FlowInstance instance, StateId? stateId = null)
        where TState : FlowState, new()
    {
        var functionId = new FlowId(Type, instance);
        return stateId is null 
            ? _stateFetcher.FetchState<TState>(functionId) 
            : _stateFetcher.FetchState<TState>(functionId, stateId);
    }
         
    
    public Task ScheduleIn(string flowInstance, TimeSpan delay) 
        => ScheduleAt(flowInstance, DateTime.UtcNow.Add(delay));
}