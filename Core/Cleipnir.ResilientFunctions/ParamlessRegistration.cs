using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;

namespace Cleipnir.ResilientFunctions;

public class ParamlessRegistration
{
    private readonly ControlPanelFactory _controlPanelFactory;
    public FunctionTypeId TypeId { get; }
    
    public Func<FunctionInstanceId, Task> Invoke { get; }
    public Func<FunctionInstanceId, Task> Schedule { get; }
    public Func<FunctionInstanceId, DateTime, Task> ScheduleAt { get; }
    public Func<IEnumerable<FunctionInstanceId>, Task> BulkSchedule { get; }
    
    private readonly StateFetcher _stateFetcher;
    public MessageWriters MessageWriters { get; }
    
    public ParamlessRegistration(
        FunctionTypeId functionTypeId,
        Func<FunctionInstanceId, Task> invoke,
        Func<FunctionInstanceId, Task> schedule,
        Func<FunctionInstanceId, DateTime, Task> scheduleAt,
        Func<IEnumerable<FunctionInstanceId>, Task> bulkSchedule,
        ControlPanelFactory controlPanelFactory, 
        MessageWriters messageWriters, 
        StateFetcher stateFetcher)
    {
        TypeId = functionTypeId;
        
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;
        BulkSchedule = bulkSchedule;
        _controlPanelFactory = controlPanelFactory;
        MessageWriters = messageWriters;
        _stateFetcher = stateFetcher;
    }

    public Task<ControlPanel?> ControlPanel(FunctionInstanceId functionInstanceId)
        => _controlPanelFactory.Create(functionInstanceId);

    public Task<TState?> GetState<TState>(FunctionInstanceId instanceId, StateId? stateId = null)
        where TState : WorkflowState, new()
    {
        var functionId = new FunctionId(TypeId, instanceId);
        return stateId is null 
            ? _stateFetcher.FetchState<TState>(functionId) 
            : _stateFetcher.FetchState<TState>(functionId, stateId);
    }
         
    
    public Task ScheduleIn(string functionInstanceId, TimeSpan delay) 
        => ScheduleAt(functionInstanceId, DateTime.UtcNow.Add(delay));
}