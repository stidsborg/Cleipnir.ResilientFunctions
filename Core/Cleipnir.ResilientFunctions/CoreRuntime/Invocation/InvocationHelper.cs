using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal class InvocationHelper<TParam, TReturn> 
{
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly bool _clearChildren;
    private readonly IFunctionStore _functionStore;
    private readonly SettingsWithDefaults _settings;
    private readonly bool _isParamlessFunction;
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly ReplicaId _replicaId;
    private readonly ResultBusyWaiter<TReturn> _resultBusyWaiter;
    private readonly IMessageClearer _messageClearer;
    private readonly MessageWatchdog _messageWatchdog;
    public UtcNow UtcNow { get; }

    private ISerializer Serializer { get; }

    public InvocationHelper(FlowType flowType, StoredType storedType, ReplicaId replicaId, bool isParamlessFunction, SettingsWithDefaults settings, IFunctionStore functionStore, ShutdownCoordinator shutdownCoordinator, ISerializer serializer, UtcNow utcNow, bool clearChildren, IMessageClearer messageClearer, MessageWatchdog messageWatchdog)
    {
        _flowType = flowType;
        _isParamlessFunction = isParamlessFunction;
        _settings = settings;

        Serializer = serializer;
        UtcNow = utcNow;
        _shutdownCoordinator = shutdownCoordinator;
        _clearChildren = clearChildren;
        _storedType = storedType;
        _replicaId = replicaId;
        _functionStore = functionStore;
        _messageClearer = messageClearer;
        _messageWatchdog = messageWatchdog;
        _resultBusyWaiter = new ResultBusyWaiter<TReturn>(_functionStore, Serializer);
    }

    public record PersistedInStoreResult(bool Created, IDisposable RunningFunction, IStorageSession? StorageSession);
    public async Task<PersistedInStoreResult> PersistFunctionInStore(
        FlowId flowId,
        StoredId storedId, 
        FlowInstance humanInstanceId, 
        TParam param, 
        DateTime? scheduleAt,
        StoredId? parent,
        ReplicaId? owner,
        InitialState? initialState)
    {
        if (!_isParamlessFunction)
            ArgumentNullException.ThrowIfNull(param);
        
        var runningFunction = _shutdownCoordinator.RegisterRunningFunction();
        try
        {
            var storedParameter = SerializeParameter(param);
            var utcNowTicks = UtcNow().Ticks;
            var effects = initialState == null
                ? null
                : MapInitialEffects(initialState.Effects, flowId);
            var messages = initialState == null
                ? null
                : MapInitialMessages(initialState.Messages);
            
            var storageState = await _functionStore.CreateFunction(
                storedId,
                humanInstanceId,
                storedParameter,
                postponeUntil: scheduleAt?.ToUniversalTime().Ticks,
                timestamp: utcNowTicks,
                parent,
                owner: scheduleAt == null ? owner : null,
                effects,
                messages
            );

            var created = storageState != null;
            if (!created) runningFunction.Dispose();
            return new PersistedInStoreResult(created, runningFunction, storageState);
        }
        catch (Exception)
        {
            runningFunction.Dispose();
            throw;
        }
    }

    public async Task<TReturn> WaitForFunctionResult(FlowId flowId, StoredId storedId, bool allowPostponedAndSuspended, TimeSpan? maxWait)
        => await _resultBusyWaiter.WaitForFunctionResult(flowId, storedId, allowPostponedAndSuspended, maxWait);
    
    public async Task PersistFailure(StoredId storedId, FatalWorkflowException exception, TParam param)
    {
        var storedException = exception.ToStoredException();

        var success = await _functionStore.FailFunction(
            storedId,
            storedException,
            timestamp: UtcNow().Ticks,
            _replicaId,
            effects: null,
            messages: null,
            storageSession: null
        );
        if (!success)
            throw UnexpectedStateException.ConcurrentModification(storedId);
    }

    public async Task<PersistResultOutcome> PersistResult(StoredId storedId, Result<TReturn> result, TParam param, IStorageSession? storageSession)
    {
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return await _functionStore.SucceedFunction(
                    storedId,
                    result: SerializeResult(result.SucceedWithValue),
                    timestamp: UtcNow().Ticks,
                    _replicaId,
                    effects: null,
                    messages: null,
                    storageSession
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Postpone:
                return await _functionStore.PostponeFunction(
                    storedId,
                    postponeUntil: result.Postpone!.DateTime.Ticks,
                    timestamp: UtcNow().Ticks,
                    _replicaId,
                    effects: null,
                    messages: null,
                    storageSession
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Success;
            case Outcome.Fail:
                return await _functionStore.FailFunction(
                    storedId,
                    storedException: result.Fail!.ToStoredException(),
                    timestamp: UtcNow().Ticks,
                    _replicaId,
                    effects: null,
                    messages: null,
                    storageSession
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Suspend:
                return await _functionStore.SuspendFunction(
                    storedId,
                    timestamp: UtcNow().Ticks,
                    _replicaId,
                    effects: null,
                    messages: null,
                    storageSession
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Reschedule;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public static void EnsureSuccess(FlowId flowId, Result<TReturn> result, bool allowPostponedOrSuspended)
    {
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return;
            case Outcome.Postpone:
                if (allowPostponedOrSuspended)
                    return;
                else
                    throw new InvocationPostponedException(flowId, result.Postpone!.DateTime);
            case Outcome.Fail:
                ExceptionDispatchInfo.Throw(result.Fail!);
                break;
            case Outcome.Suspend:
                if (allowPostponedOrSuspended)
                    return;
                else
                    throw new InvocationSuspendedException(flowId);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public async Task PublishCompletionMessageToParent(StoredId? parent, FlowId childId, Result<TReturn> result)
    {
        if (parent == null)
            return;
        
        var msg = result.Outcome switch
        {
            Outcome.Succeed => new FlowCompleted(childId, Result: SerializeResult(result.SucceedWithValue), Failed: false),
            Outcome.Fail => new FlowCompleted(childId, Result: null, Failed: true),
            _ => default
        };

        if (msg == null)
            return;

        var content = Serializer.Serialize(msg, msg.GetType());
        var type = Serializer.SerializeType(msg.GetType());
        var storedMessage = new StoredMessage(content, type, Position: 0, IdempotencyKey: $"FlowCompleted:{childId}", Replica: _replicaId);
        await _functionStore.MessageStore.AppendMessages([new StoredIdAndMessage(parent, storedMessage)]);

        // Wake the MessageWatchdog so the waiting parent receives the completion now rather than on the next poll.
        _messageWatchdog.Notify();
    }

    public async Task<RestartedFunction?> RestartFunction(StoredId flowId)
    {
        var restarted = await _functionStore.ClaimFunction(
            flowId,
            _replicaId
        );
        if (restarted == null)
            return null;

        // The restart does not pull the flow's messages: store-resident messages are fetched and pushed by the
        // MessageWatchdog (woken here so they arrive now rather than on the next poll), while messages inlined
        // into the effect state while the flow was completed travel in the effect snapshot handed over below and
        // are staged by the QueueManager at initialization.
        _messageWatchdog.Notify();

        return new RestartedFunction(
            restarted.StoredFlow,
            restarted.Effects,
            StoredMessages: [],
            restarted.StorageSession
        );
    }
    
    public async Task<PreparedReInvocation> PrepareForReInvocation(StoredId storedId, RestartedFunction restartedFunction)
    {
        var (sf, effects, messages, storageSession) = restartedFunction;
        var flowId = new FlowId(_flowType, sf.InstanceId);

        var runningFunction = _shutdownCoordinator.RegisterRunningFunction();
        
        try
        {
            var param = sf.Parameter == null
                ? default
                : (TParam)Serializer.Deserialize(sf.Parameter, typeof(TParam));

            return new PreparedReInvocation(
                flowId,
                param,
                effects, 
                messages,
                runningFunction,
                sf.ParentId,
                storageSession
            );
        }
        catch (DeserializationException e)
        {
            runningFunction.Dispose();
            sf = await _functionStore.GetFunction(storedId);
            if (sf == null)
                throw UnexpectedStateException.NotFound(flowId);
            
            await _functionStore.FailFunction(
                storedId,
                storedException: FatalWorkflowException.CreateNonGeneric(flowId, e).ToStoredException(),
                timestamp: UtcNow().Ticks,
                _replicaId,
                effects: null,
                messages: null,
                storageSession: null
            );
            throw;
        }
        catch (Exception)
        {
            runningFunction.Dispose();
            throw;
        }
    }

    internal record PreparedReInvocation(
        FlowId FlowId,
        TParam? Param,
        IReadOnlyList<StoredEffect> Effects,
        IReadOnlyList<StoredMessage> Messages,
        IDisposable RunningFunction,
        StoredId? Parent,
        IStorageSession? StorageSession
    );
    
    public async Task<bool> SetFunctionState(
        StoredId storedId,
        Status status,
        TParam param,
        TReturn? result,
        long expires,
        FatalWorkflowException? exception,
        ReplicaId? expectedReplicaId
    )
    {
        return await _functionStore.SetFunctionState(
            storedId,
            status,
            param: SerializeParameter(param),
            result: SerializeResult(result),
            exception == null ? null : exception.ToStoredException(),
            expires,
            expectedReplicaId
        );
    }

    public async Task<bool> SaveControlPanelChanges(
        StoredId storedId, 
        TParam param, 
        TReturn? @return,
        ReplicaId? expectedReplica)
    {
        return await _functionStore.SetParameters(
            storedId,
            param: SerializeParameter(param),
            result: SerializeResult(@return),
            expectedReplica
        );
    }

    public async Task Delete(StoredId storedId) => await _functionStore.DeleteFunction(storedId);

    public async Task<FunctionState<TParam, TReturn>?> GetFunction(StoredId storedId, FlowId flowId)
    {
        var sf = await _functionStore.GetFunction(storedId);
        if (sf == null)
            return null;

        var results = await _functionStore.GetResults([storedId]);
        var resultBytes = results.TryGetValue(storedId, out var rb) ? rb : null;

        return new FunctionState<TParam, TReturn>(
            sf.Status,
            sf.Expires,
            sf.OwnerId,
            Param:
                sf.Parameter == null
                ? default
                : (TParam)Serializer.Deserialize(sf.Parameter, typeof(TParam)),
            Result: resultBytes == null
                ? default
                : (TReturn)Serializer.Deserialize(resultBytes, typeof(TReturn)),
            FatalWorkflowException: sf.Exception == null
                ? null
                : FatalWorkflowException.Create(flowId, sf.Exception)
        );
    }

    public async Task<InnerScheduled<TReturn>> BulkSchedule(IReadOnlyList<BulkWork<TParam>> work, bool? detach = null)
    {
        var parent = GetAndEnsureParent(detach);
        if (parent != null)
        {
            var marked = await parent.Effect.Mark(flush: true); //todo should flush be true
            if (!marked)
                return CreateInnerScheduled(
                    work.Select(w => new FlowId(_flowType, w.Instance)).ToList(),
                    parent,
                    detach
                );    
        }

        await _functionStore.BulkScheduleFunctions(
            work.Select(bw =>
            {
                byte[]? paramBytes = null;
                if (!_isParamlessFunction && bw.Param != null)
                    paramBytes = Serializer.Serialize(bw.Param, typeof(TParam));
                return new IdWithParam(
                    StoredId.Create(_storedType, bw.Instance),
                    bw.Instance,
                    paramBytes
                );
            }),
            parent?.StoredId
        );
        return CreateInnerScheduled(
            work.Select(w => new FlowId(_flowType, w.Instance)).ToList(),
            parent,
            detach
        );
    }
    
    public MessageWriter CreateMessageWriter(StoredId storedId)
        => new MessageWriter(storedId, _functionStore.MessageStore, Serializer, _replicaId, _messageWatchdog);

    public Effect CreateEffect(StoredId storedId, FlowId flowId, IReadOnlyList<StoredEffect> storedEffects, FlowTimeouts flowTimeouts, IStorageSession? storageSession, FlowExecutionState flowExecutionState)
    {
        var effectsStore = _functionStore.EffectsStore;

        var effectResults = new EffectResults(
            flowId,
            storedId,
            storedEffects,
            effectsStore,
            Serializer,
            storageSession,
            _clearChildren
        );

       var effect = new Effect(effectResults, UtcNow, flowTimeouts, flowExecutionState);
       return effect;
    }

    public async Task<ExistingEffects> CreateExistingEffects(FlowId flowId)
    {
        var storedId = MapToStoredId(flowId);
        var storedEffects = await _functionStore.EffectsStore.GetEffectResults(storedId);
        return new ExistingEffects(storedId, flowId, _functionStore.EffectsStore, Serializer, storedEffects);
    }
    public ExistingMessages CreateExistingMessages(FlowId flowId) => new(MapToStoredId(flowId), _functionStore.MessageStore, _functionStore.EffectsStore, Serializer, _replicaId);

    public QueueManager CreateQueueManager(FlowId flowId, StoredId storedId, Effect effect, FlowExecutionState flowExecutionState, FlowTimeouts timeouts, UnhandledExceptionHandler unhandledExceptionHandler)
        => new(flowId, storedId, Serializer, effect, flowExecutionState, unhandledExceptionHandler, timeouts, UtcNow, _settings, _messageClearer);

    public StoredId MapToStoredId(FlowId flowId) => StoredId.Create(_storedType, flowId.Instance.Value);
    
    private byte[]? SerializeParameter(TParam param)
    {
        if (typeof(TParam) == typeof(Unit))
            return null;

        if (param is null)
            return null;

        return Serializer.Serialize(param, typeof(TParam));
    }
    
    private byte[]? SerializeResult(TReturn? result)
    {
        if (typeof(TReturn) == typeof(Unit))
            return null;

        if (result is null)
            return null;

        return Serializer.Serialize(result, typeof(TReturn));
    }

    public InnerScheduled<TReturn> CreateInnerScheduled(List<FlowId> scheduledIds, Workflow? parentWorkflow, bool? detach, Task<TReturn>? task = null)
        => new(
            _storedType,
            scheduledIds,
            parentWorkflow: detach == false ? null : parentWorkflow,
            Serializer,
            _resultBusyWaiter,
            task
        );
    
    public Workflow? GetAndEnsureParent(bool? detach)
    {
        if (detach == true)
            return null;
        
        var parentWorkflow = CurrentFlow.Workflow;
        if (parentWorkflow == null && detach == false) 
            throw new InvalidOperationException("Cannot start an attached flow without a parent");
        
        return parentWorkflow;
    }
    
    public IReadOnlyList<StoredEffect> MapInitialEffects(IEnumerable<InitialEffect> initialEffects, FlowId flowId)
    => initialEffects
        .Select(e =>
        {
            if (e.Exception == null)
            {
                byte[]? resultBytes = null;
                if (e.Value != null)
                    resultBytes = Serializer.Serialize(e.Value, e.Value.GetType());
                return new StoredEffect(
                    e.Id,
                    e.Status ?? WorkStatus.Completed,
                    Result: resultBytes,
                    StoredException: null,
                    Alias: e.Alias ?? e.Id.Serialize().ToStringValue());
            }
            return new StoredEffect(
                e.Id,
                WorkStatus.Failed,
                Result: null,
                StoredException: FatalWorkflowException.CreateNonGeneric(flowId, e.Exception).ToStoredException(),
                Alias: e.Alias
            );
        }).ToList();

    public IReadOnlyList<StoredMessage> MapInitialMessages(IEnumerable<MessageAndIdempotencyKey> initialMessages)
        => initialMessages.Select(m =>
        {
            var content = Serializer.Serialize(m.Message, m.Message.GetType());
            var type = Serializer.SerializeType(m.Message.GetType());
            return new StoredMessage(content, type, Position: 0, Replica: _replicaId, IdempotencyKey: m.IdempotencyKey);
        }).ToList();

    internal IReadOnlyList<StoredMessage> AddPositionsToMessages(IReadOnlyList<StoredMessage> messages)
    {
        var position = 0L;
        return messages.Select(m => m with { Position = position++ }).ToList();
    }

    public async Task<bool> Reschedule(StoredId id, TParam param)
    {
        return await _functionStore.PostponeFunction(
            id,
            postponeUntil: 0,
            timestamp: UtcNow().Ticks,
            _replicaId,
            effects: null,
            messages: null,
            storageSession: null
        );
    }
}