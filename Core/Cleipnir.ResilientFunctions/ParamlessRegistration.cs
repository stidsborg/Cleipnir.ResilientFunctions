using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public class ParamlessRegistration : BaseRegistration
{
    private readonly Invoker<Unit, Unit> _invoker;
    private readonly ControlPanelFactory _controlPanelFactory;
    public FlowType Type { get; }

    public MessageWriters MessageWriters { get; }
    private readonly IFunctionStore _functionStore;

    public ParamlessRegistration(
        FlowType flowType,
        StoredType storedType,
        IFunctionStore functionStore,
        Invoker<Unit, Unit> invoker,
        ControlPanelFactory controlPanelFactory,
        MessageWriters messageWriters,
        Postman postman,
        UtcNow utcNow
    ) : base(storedType, postman, functionStore, utcNow)
    {
        Type = flowType;
        _invoker = invoker;
        _controlPanelFactory = controlPanelFactory;
        MessageWriters = messageWriters;
        _functionStore = functionStore;
    }

    public async Task Invoke(FlowInstance flowInstance, InitialState? initialState = null)
        => await (await _invoker.ScheduleInvoke(flowInstance.Value, param: Unit.Instance, detach: null, initialState))
            .Completion(allowPostponedAndSuspended: false);

    public async Task<Scheduled> Schedule(FlowInstance flowInstance, bool? detach = null, InitialState? initialState = null)
        => (await _invoker.ScheduleInvoke(flowInstance.Value, param: Unit.Instance, detach, initialState)).ToScheduledWithoutResult();

    public async Task<Scheduled> ScheduleAt(FlowInstance flowInstance, DateTime delayUntil, bool? detach = null)
        => (await _invoker.ScheduleAt(flowInstance.Value, param: Unit.Instance, delayUntil, detach)).ToScheduledWithoutResult();

    public async Task<BulkScheduled> BulkSchedule(IEnumerable<FlowInstance> instances, bool? detach = null)
        => (await _invoker.BulkSchedule(instances.Select(id => new BulkWork<Unit>(id.Value, Unit.Instance)), detach)).ToScheduledWithoutResults();

    public Task<ControlPanel?> ControlPanel(FlowInstance flowInstance)
        => _controlPanelFactory.Create(flowInstance);

    public Task<Scheduled> ScheduleIn(FlowInstance flowInstance, TimeSpan delay, bool? detach = null)
        => ScheduleAt(flowInstance, delayUntil: UtcNow().Add(delay), detach);

    public async Task SendMessage<T>(
        FlowInstance flowInstance,
        T message,
        bool create = true,
        string? idempotencyKey = null) where T : class
    {
        if (create)
        {
            var sf = await _functionStore.GetFunction(StoredId.Create(StoredType, flowInstance.Value));
            if (sf is null)
                await Schedule(flowInstance);
        }

        await Postman.SendMessage(StoredId.Create(StoredType, flowInstance.Value), message, idempotencyKey);
    }

    public async Task SendMessages(IReadOnlyList<BatchedMessage> messages)
        => await Postman.SendMessages(messages);
}