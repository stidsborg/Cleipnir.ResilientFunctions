using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public static class Paramless
{
    public delegate Task Invoke(FlowInstance flowInstance, InitialState? initialState = null);
    public delegate Task<Scheduled> Schedule(FlowInstance flowInstance, bool? detach = null, InitialState? initialState = null);
    public delegate Task<BulkScheduled> BulkSchedule(IEnumerable<FlowInstance> instances, bool? detach = null);
    public delegate Task<Scheduled> ScheduleAt(
        FlowInstance flowInstance,
        DateTime delayUntil,
        bool? detach = null
    );
}

public class ParamlessRegistration : BaseRegistration
{
    private readonly ControlPanelFactory _controlPanelFactory;
    public FlowType Type { get; }
    
    public Paramless.Invoke Invoke { get; }
    public Paramless.Schedule Schedule { get; }
    public Paramless.ScheduleAt ScheduleAt { get; }
    public Paramless.BulkSchedule BulkSchedule { get; }
    
    private readonly StateFetcher _stateFetcher;
    public MessageWriters MessageWriters { get; }
    private readonly IFunctionStore _functionStore;
    
    public ParamlessRegistration(
        FlowType flowType,
        StoredType storedType,
        IFunctionStore functionStore,
        Paramless.Invoke invoke,
        Paramless.Schedule schedule,
        Paramless.ScheduleAt scheduleAt,
        Paramless.BulkSchedule bulkSchedule,
        ControlPanelFactory controlPanelFactory, 
        MessageWriters messageWriters, 
        StateFetcher stateFetcher,
        Postman postman,
        UtcNow utcNow
    ) : base(storedType, postman, functionStore, utcNow)
    {
        Type = flowType;
        
        Invoke = invoke;
        Schedule = schedule;
        ScheduleAt = scheduleAt;
        BulkSchedule = bulkSchedule;
        _controlPanelFactory = controlPanelFactory;
        MessageWriters = messageWriters;
        _stateFetcher = stateFetcher;
        _functionStore = functionStore;
    }
    
    public Task<ControlPanel?> ControlPanel(FlowInstance flowInstance)
        => _controlPanelFactory.Create(flowInstance);

    public Task<TState?> GetState<TState>(FlowInstance instance, StateId? stateId = null)
        where TState : FlowState, new()
    {
        return stateId is null 
            ? _stateFetcher.FetchState<TState>(instance) 
            : _stateFetcher.FetchState<TState>(instance, stateId);
    }
    
    public Task ScheduleIn(string flowInstance, TimeSpan delay, bool? detach = null) 
        => ScheduleAt(flowInstance, delayUntil: UtcNow().Add(delay), detach);

    public async Task<Finding> SendMessage<T>(
        FlowInstance flowInstance,
        T message,
        bool create = true,
        string? idempotencyKey = null) where T : notnull
    {
        if (create)
        {
            var sf = await _functionStore.GetFunction(new StoredId(StoredType, flowInstance.ToStoredInstance()));
            if (sf is null)
                await Schedule(flowInstance);    
        }
        
        return await Postman.SendMessage(flowInstance.Value.ToStoredInstance(), message, idempotencyKey);
    }
    
    public async Task SendMessages(IReadOnlyList<BatchedMessage> messages, bool interrupt = true)
        => await Postman.SendMessages(messages, interrupt);
}