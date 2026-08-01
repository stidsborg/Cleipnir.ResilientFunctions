using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions;

public class FuncRegistration<TParam, TReturn> : BaseRegistration where TParam : notnull
{
    public FlowType Type { get; }

    private readonly Invoker<TParam, TReturn> _invoker;
    private readonly ControlPanelFactory<TParam,TReturn> _controlPanelFactory;
    private readonly MessageSender _messageSender;

    internal FuncRegistration(
        FlowType flowType,
        StoredType storedType,
        Invoker<TParam, TReturn> invoker,
        ControlPanelFactory<TParam, TReturn> controlPanelFactory,
        MessageSender messageSender,
        UtcNow utcNow
    ) : base(storedType, utcNow)
    {
        Type = flowType;
        _invoker = invoker;

        _controlPanelFactory = controlPanelFactory;
        _messageSender = messageSender;
    }

    public async Task<TReturn> Run(FlowInstance flowInstance, TParam param, InitialState? initialState = null)
        => (await (await _invoker.ScheduleInvoke(flowInstance, param, detach: null, initialState))
            .Completion(allowPostponedAndSuspended: false)).First();

    public async Task<Scheduled<TReturn>> Schedule(FlowInstance flowInstance, TParam param, bool? detach = null, InitialState? initialState = null)
        => (await _invoker.ScheduleInvoke(flowInstance, param, detach, initialState)).ToScheduledWithResult();

    public async Task<Scheduled<TReturn>> ScheduleAt(FlowInstance flowInstance, TParam param, DateTime delayUntil, bool? detach = null)
        => (await _invoker.ScheduleAt(flowInstance, param, delayUntil, detach)).ToScheduledWithResult();

    public Task<Scheduled<TReturn>> ScheduleIn(FlowInstance flowInstance, TParam param, TimeSpan delay, bool? detach = null)
        => ScheduleAt(flowInstance, param, delayUntil: UtcNow().Add(delay), detach);

    public async Task<BulkScheduled<TReturn>> BulkSchedule(IEnumerable<BulkWork<TParam>> instances, bool? detach = null)
        => (await _invoker.BulkSchedule(instances, detach)).ToScheduledWithResults();

    public Task<ControlPanel<TParam, TReturn>?> ControlPanel(FlowInstance flowInstance)
        => _controlPanelFactory.Create(flowInstance);

    public async Task SendMessage<T>(
        FlowInstance flowInstance,
        T message,
        string? idempotencyKey = null,
        string? sender = null,
        string? receiver = null
    ) where T : class => await _messageSender.SendMessage(MapToStoredId(flowInstance), message, idempotencyKey, sender, receiver);

    public async Task SendMessages(IReadOnlyList<BatchedMessage> messages)
        => await _messageSender.SendMessages(StoredType, messages);
}