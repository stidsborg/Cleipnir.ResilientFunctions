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

            // Initial messages are written into the flow's effect state as staged-message children rather than as
            // message-store rows: the QueueManager stages and delivers them at initialization exactly as it does for
            // control-panel appended messages. This keeps them out of the watchdog-visible message-store rows
            // entirely.
            IReadOnlyList<StoredEffect>? effects = initialState == null
                ? null
                : MapInitialEffectsAndMessages(initialState, flowId);

            var storageState = await _functionStore.CreateFunction(
                storedId,
                humanInstanceId,
                storedParameter,
                postponeUntil: scheduleAt?.ToUniversalTime().Ticks,
                timestamp: utcNowTicks,
                parent,
                owner: scheduleAt == null ? owner : null,
                effects
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

        var success = await _functionStore.SetStatus(
            storedId,
            Status.Failed,
            result: null,
            storedException,
            expires: 0,
            timestamp: UtcNow().Ticks,
            _replicaId,
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
                return await _functionStore.SetStatus(
                    storedId,
                    Status.Succeeded,
                    result: SerializeResult(result.SucceedWithValue),
                    storedException: null,
                    expires: 0,
                    timestamp: UtcNow().Ticks,
                    _replicaId,
                    storageSession
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Postpone:
                return await _functionStore.SetStatus(
                    storedId,
                    Status.Postponed,
                    result: null,
                    storedException: null,
                    expires: result.Postpone!.DateTime.Ticks,
                    timestamp: UtcNow().Ticks,
                    _replicaId,
                    storageSession
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Success;
            case Outcome.Fail:
                return await _functionStore.SetStatus(
                    storedId,
                    Status.Failed,
                    result: null,
                    storedException: result.Fail!.ToStoredException(),
                    expires: 0,
                    timestamp: UtcNow().Ticks,
                    _replicaId,
                    storageSession
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Suspend:
                return await _functionStore.SetStatus(
                    storedId,
                    Status.Suspended,
                    result: null,
                    storedException: null,
                    expires: 0,
                    timestamp: UtcNow().Ticks,
                    _replicaId,
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
            
            await _functionStore.SetStatus(
                storedId,
                Status.Failed,
                result: null,
                storedException: FatalWorkflowException.CreateNonGeneric(flowId, e).ToStoredException(),
                expires: 0,
                timestamp: UtcNow().Ticks,
                _replicaId,
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
        var effectResults = new EffectResults(
            flowId,
            storedId,
            storedEffects,
            _functionStore,
            Serializer,
            owner: _replicaId,
            storageSession,
            _clearChildren
        );

       var effect = new Effect(effectResults, UtcNow, flowTimeouts, flowExecutionState);
       return effect;
    }

    public async Task<ExistingEffects> CreateExistingEffects(FlowId flowId)
    {
        var storedId = MapToStoredId(flowId);
        var storedEffects = (await _functionStore.GetFunction(storedId))?.Effects ?? [];
        return new ExistingEffects(storedId, flowId, _functionStore, Serializer, storedEffects);
    }
    public ExistingMessages CreateExistingMessages(FlowId flowId) => new(MapToStoredId(flowId), _functionStore.MessageStore, _functionStore, Serializer);

    public QueueManager CreateQueueManager(FlowId flowId, StoredId storedId, Effect effect, FlowExecutionState flowExecutionState, FlowTimeouts timeouts, UnhandledExceptionHandler unhandledExceptionHandler)
        => new(flowId, storedId, Serializer, effect, flowExecutionState, unhandledExceptionHandler, timeouts, UtcNow, _messageClearer);

    internal TimeSpan MessagesDefaultMaxWaitForCompletion => _settings.MessagesDefaultMaxWaitForCompletion;

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
    
    /// <summary>
    /// Maps a flow's initial state to the effects persisted at creation: the user-supplied initial effects plus one
    /// staged-message child per initial message (see <see cref="MapInitialMessagesToEffects"/>). Used both to
    /// persist the effects (CreateFunction) and to seed the invocation's in-memory effect state, so the two stay
    /// identical and the QueueManager finds the messages.
    /// </summary>
    public IReadOnlyList<StoredEffect> MapInitialEffectsAndMessages(InitialState initialState, FlowId flowId)
    {
        var initialEffects = MapInitialEffects(initialState.Effects, flowId);
        var initialMessageEffects = MapInitialMessagesToEffects(initialState.Messages);
        return [..initialEffects, ..initialMessageEffects];
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

    /// <summary>
    /// Encodes the flow's initial messages as staged-message children (under
    /// <see cref="QueueManager.StagedMessagesRoot"/>) so they live in the flow's effect state instead of as
    /// message-store rows. They are written exactly as control-panel appended messages are: row-less, carrying no
    /// store position at all - there is no row for one to address. The QueueManager stages them from their children
    /// at initialization and delivers them ahead of any store-backed message in child order, so the order they were
    /// supplied in is the order they arrive in.
    /// </summary>
    public IReadOnlyList<StoredEffect> MapInitialMessagesToEffects(IEnumerable<MessageAndIdempotencyKey> initialMessages)
    {
        // A message staged from its own child is admitted on sight, so duplicate idempotency keys are resolved
        // here rather than at delivery: the whole batch is in hand at creation, so the first message per key wins
        // and the rest never become children at all. Messages without a key are always distinct.
        var claimedIdempotencyKeys = new HashSet<string>();
        var effects = new List<StoredEffect>();

        foreach (var message in initialMessages)
        {
            if (message.IdempotencyKey is not null && !claimedIdempotencyKeys.Add(message.IdempotencyKey))
                continue;

            var content = Serializer.Serialize(message.Message, message.Message.GetType());
            var type = Serializer.SerializeType(message.Message.GetType());
            var encodedMessage = PendingMessages.EncodeMessage(
                new IncomingMessage(content, type, Position: null, IdempotencyKey: message.IdempotencyKey)
            );

            effects.Add(
                StoredEffect.CreateCompleted(
                    QueueManager.StagedMessagesRoot.CreateChild(effects.Count),
                    Serializer.Serialize(encodedMessage, typeof(byte[])),
                    alias: null
                )
            );
        }

        return effects;
    }

    internal IReadOnlyList<StoredMessage> AddPositionsToMessages(IReadOnlyList<StoredMessage> messages)
    {
        var position = 0L;
        return messages.Select(m => m with { Position = position++ }).ToList();
    }

    public async Task<bool> Reschedule(StoredId id, TParam param)
    {
        return await _functionStore.SetStatus(
            id,
            Status.Postponed,
            result: null,
            storedException: null,
            expires: 0,
            timestamp: UtcNow().Ticks,
            _replicaId,
            storageSession: null
        );
    }
}