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
    public MessageWriters MessageWriters { get; }

    public FuncRegistration(
        FlowType flowType,
        StoredType storedType,
        IFunctionStore functionStore,
        Invoker<TParam, TReturn> invoker,
        ControlPanelFactory<TParam, TReturn> controlPanelFactory,
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

    public async Task<TReturn> Invoke(FlowInstance flowInstance, TParam param, InitialState? initialState = null)
        => (await (await _invoker.ScheduleInvoke(flowInstance, param, detach: null, initialState))
            .Completion(allowPostponedAndSuspended: false)).First();

    public async Task<Scheduled<TReturn>> Schedule(FlowInstance flowInstance, TParam param, bool? detach = null, InitialState? initialState = null)
        => (await _invoker.ScheduleInvoke(flowInstance, param, detach, initialState)).ToScheduledWithResult();

    public async Task<Scheduled<TReturn>> ScheduleAt(FlowInstance flowInstance, TParam param, DateTime delayUntil, bool? detach = null)
        => (await _invoker.ScheduleAt(flowInstance, param, delayUntil, detach)).ToScheduledWithResult();

    public async Task<BulkScheduled<TReturn>> BulkSchedule(IEnumerable<BulkWork<TParam>> instances, bool? detach = null)
        => (await _invoker.BulkSchedule(instances, detach)).ToScheduledWithResults();

    public Task<ControlPanel<TParam, TReturn>?> ControlPanel(FlowInstance flowInstance)
        => _controlPanelFactory.Create(flowInstance);

    public async Task SendMessage<T>(
        FlowInstance flowInstance,
        T message,
        string? idempotencyKey = null
    ) where T : class => await Postman.SendMessage(StoredId.Create(StoredType, flowInstance.Value), message, idempotencyKey);

    public async Task SendMessages(IReadOnlyList<BatchedMessage> messages)
        => await Postman.SendMessages(messages);
}