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

public class ActionRegistration<TParam> : BaseRegistration where TParam : notnull
{
    private readonly Invoker<TParam, Unit> _invoker;
    private readonly ControlPanelFactory<TParam> _controlPanelFactory;
    public FlowType Type { get; }

    public MessageWriters MessageWriters { get; }

    public ActionRegistration(
        FlowType flowType,
        StoredType storedType,
        IFunctionStore functionStore,
        Invoker<TParam, Unit> invoker,
        ControlPanelFactory<TParam> controlPanelFactory,
        MessageWriters messageWriters,
        Postman postman,
        UtcNow utcNow
    ) : base(storedType, postman, functionStore, utcNow)
    {
        Type = flowType;
        _invoker = invoker;
        _controlPanelFactory = controlPanelFactory;
        MessageWriters = messageWriters;
    }

    public async Task Invoke(FlowInstance flowInstance, TParam param, InitialState? initialState = null)
        => await (await _invoker.ScheduleInvoke(flowInstance, param, detach: null, initialState))
            .Completion(allowPostponedAndSuspended: false);

    public async Task<Scheduled> Schedule(FlowInstance flowInstance, TParam param, bool? detach = null, InitialState? initialState = null)
        => (await _invoker.ScheduleInvoke(flowInstance, param, detach, initialState)).ToScheduledWithoutResult();

    public async Task<Scheduled> ScheduleAt(FlowInstance flowInstance, TParam param, DateTime delayUntil, bool? detach = null)
        => (await _invoker.ScheduleAt(flowInstance, param, delayUntil, detach)).ToScheduledWithoutResult();

    public async Task<BulkScheduled> BulkSchedule(IEnumerable<BulkWork<TParam>> instances, bool? detach = null)
        => (await _invoker.BulkSchedule(instances, detach)).ToScheduledWithoutResults();

    public Task<ControlPanel<TParam>?> ControlPanel(FlowInstance flowInstance)
        => _controlPanelFactory.Create(flowInstance);

    public Task<Scheduled> ScheduleIn(FlowInstance flowInstance, TParam param, TimeSpan delay, bool? detach = null)
        => ScheduleAt(flowInstance, param, delayUntil: UtcNow().Add(delay), detach);

    public async Task SendMessage<T>(
        FlowInstance flowInstance,
        T message,
        string? idempotencyKey = null
    ) where T : class => await Postman.SendMessage(StoredId.Create(StoredType, flowInstance.Value), message, idempotencyKey);

    public async Task SendMessages(IReadOnlyList<BatchedMessage> messages)
        => await Postman.SendMessages(messages);
}