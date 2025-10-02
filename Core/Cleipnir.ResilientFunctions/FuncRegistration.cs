using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public static class FuncRegistration
{
    public delegate Task<TReturn> Invoke<in TParam, TReturn>(
        FlowInstance flowInstance,
        TParam param,
        InitialState? initialState = null
    ) where TParam : notnull;

    public delegate Task<Scheduled<TReturn>> Schedule<in TParam, TReturn>(
        FlowInstance flowInstance,
        TParam param,
        bool? detach = null,
        InitialState? initialState = null
    ) where TParam : notnull;

    public delegate Task<Scheduled<TReturn>> ScheduleAt<in TParam, TReturn>(
        FlowInstance flowInstance,
        TParam param,
        DateTime delayUntil,
        bool? detach = null
    ) where TParam : notnull;
    
    public delegate Task<BulkScheduled<TReturn>> BulkSchedule<TParam, TReturn>(IEnumerable<BulkWork<TParam>> instances, bool? detach = null) where TParam : notnull;
}

public class FuncRegistration<TParam, TReturn> : BaseRegistration where TParam : notnull
{
    public FlowType Type { get; }
    
    public FuncRegistration.Invoke<TParam, TReturn> Invoke { get; }
    public FuncRegistration.Schedule<TParam, TReturn> Schedule { get; }
    public FuncRegistration.ScheduleAt<TParam, TReturn> ScheduleAt { get; }
    public FuncRegistration.BulkSchedule<TParam, TReturn> BulkSchedule { get; } 
    
    private readonly ControlPanelFactory<TParam,TReturn> _controlPanelFactory;
    public MessageWriters MessageWriters { get; }

    public FuncRegistration(
        FlowType flowType,
        StoredType storedType,
        IFunctionStore functionStore,
        FuncRegistration.Invoke<TParam, TReturn> invoke,
        FuncRegistration.Schedule<TParam, TReturn> schedule,
        FuncRegistration.ScheduleAt<TParam, TReturn> scheduleAt,
        FuncRegistration.BulkSchedule<TParam, TReturn> bulkSchedule,
        ControlPanelFactory<TParam, TReturn> controlPanelFactory, 
        MessageWriters messageWriters, 
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
    }

    public Task<ControlPanel<TParam, TReturn>?> ControlPanel(FlowInstance flowInstance)
        => _controlPanelFactory.Create(flowInstance);

    public Task ScheduleIn(string flowInstance, TParam param, TimeSpan delay, bool? detach = null) 
        => ScheduleAt(flowInstance, param, delayUntil: UtcNow().Add(delay), detach);
    
    public async Task SendMessage<T>(
        FlowInstance flowInstance,
        T message,
        string? idempotencyKey = null
    ) where T : notnull => await Postman.SendMessage(StoredId.Create(StoredType, flowInstance.Value), message, idempotencyKey);
    
    public async Task SendMessages(IReadOnlyList<BatchedMessage> messages, bool interrupt = true)
        => await Postman.SendMessages(messages, interrupt);
}