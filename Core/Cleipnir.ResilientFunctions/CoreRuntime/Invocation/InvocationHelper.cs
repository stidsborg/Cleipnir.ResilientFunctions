using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
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
    public UtcNow UtcNow { get; }

    private ISerializer Serializer { get; }

    public InvocationHelper(FlowType flowType, StoredType storedType, ReplicaId replicaId, bool isParamlessFunction, SettingsWithDefaults settings, IFunctionStore functionStore, ShutdownCoordinator shutdownCoordinator, ISerializer serializer, UtcNow utcNow, bool clearChildren)
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
                leaseExpiration: utcNowTicks + _settings.LeaseLength.Ticks,
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
        var storedException = Serializer.SerializeException(exception);

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
                    storedException: Serializer.SerializeException(result.Fail!),
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

        Serializer.Serialize(msg, out var content, out var type);
        var storedMessage = new StoredMessage(content, type, Position: 0, IdempotencyKey: $"FlowCompleted:{childId}");
        await _functionStore.MessageStore.AppendMessage(parent, storedMessage);
        await _functionStore.Interrupt(parent);
    }

    public async Task<RestartedFunction?> RestartFunction(StoredId flowId)
    {
        var restarted = await _functionStore.RestartExecution(
            flowId,
            _replicaId
        );

        return restarted != null
            ? new RestartedFunction(
                restarted.StoredFlow, 
                restarted.Effects,
                restarted.Messages,
                restarted.StorageSession
            ) 
            : null;
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
                storedException: Serializer.SerializeException(FatalWorkflowException.CreateNonGeneric(flowId, e)),
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
            exception == null ? null : Serializer.SerializeException(exception),
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

    public async Task Interrupt(IReadOnlyList<StoredId> storedIds)
    {
        if (storedIds.Count == 0)
            return;

        await _functionStore.Interrupt(storedIds);
    }
    
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
                : Serializer.DeserializeException(flowId, sf.Exception)
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
                new IdWithParam(
                    StoredId.Create(_storedType, bw.Instance),
                    bw.Instance,
                    _isParamlessFunction ? null : Serializer.Serialize(bw.Param)
                )
            ),
            parent?.StoredId
        );
        return CreateInnerScheduled(
            work.Select(w => new FlowId(_flowType, w.Instance)).ToList(),
            parent,
            detach
        );
    }
    
    public MessageWriter CreateMessageWriter(StoredId storedId)
        => new MessageWriter(storedId, _functionStore.MessageStore, Serializer);

    public Effect CreateEffect(StoredId storedId, FlowId flowId, IReadOnlyList<StoredEffect> storedEffects, FlowMinimumTimeout flowMinimumTimeout, IStorageSession? storageSession)
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
        
       var effect = new Effect(effectResults, UtcNow, flowMinimumTimeout);
       return effect;
    }
    
    public Correlations CreateCorrelations(FlowId flowId)
    {
        var correlationStore = _functionStore.CorrelationStore;
        return new Correlations(MapToStoredId(flowId), correlationStore);
    }

    public async Task<ExistingEffects> CreateExistingEffects(FlowId flowId)
    {
        var storedId = MapToStoredId(flowId);
        var storedEffects = await _functionStore.EffectsStore.GetEffectResults(storedId);
        return new ExistingEffects(storedId, flowId, _functionStore.EffectsStore, Serializer, storedEffects);
    }
    public ExistingMessages CreateExistingMessages(FlowId flowId) => new(MapToStoredId(flowId), _functionStore.MessageStore, Serializer);
    public async Task<ExistingSemaphores> CreateExistingSemaphores(FlowId flowId)
    {
        var existingEffects = await CreateExistingEffects(flowId);
        return new ExistingSemaphores(MapToStoredId(flowId), _functionStore, existingEffects);
    }

    public DistributedSemaphores CreateSemaphores(StoredId storedId, Effect effect)
        => new(effect, _functionStore.SemaphoreStore, storedId, Interrupt);

    public QueueManager CreateQueueManager(FlowId flowId, StoredId storedId, Effect effect, FlowMinimumTimeout minimumTimeout, UnhandledExceptionHandler unhandledExceptionHandler)
        => new(flowId, storedId, _functionStore.MessageStore, Serializer, effect, unhandledExceptionHandler, minimumTimeout, UtcNow, _settings);

    public StoredId MapToStoredId(FlowId flowId) => StoredId.Create(_storedType, flowId.Instance.Value);
    
    private byte[]? SerializeParameter(TParam param)
    {
        if (typeof(TParam) == typeof(Unit))
            return null;
        
        return param is null
            ? null 
            : Serializer.Serialize(param);
    }
    
    private byte[]? SerializeResult(TReturn? result)
    {
        if (typeof(TReturn) == typeof(Unit))
            return null;
        
        return result is null
            ? null 
            : Serializer.Serialize(result);
    }

    public InnerScheduled<TReturn> CreateInnerScheduled(List<FlowId> scheduledIds, Workflow? parentWorkflow, bool? detach)
        => new(
            _storedType,
            scheduledIds,
            parentWorkflow: detach == false ? null : parentWorkflow,
            Serializer,
            _resultBusyWaiter
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
            e.Exception == null
                ? new StoredEffect(
                    e.Id.ToEffectId(),
                    e.Status ?? WorkStatus.Completed,
                    Result: Serializer.Serialize(e.Value, e.Value?.GetType() ?? typeof(object)),
                    StoredException: null,
                    Alias: e.Alias ?? e.Id.ToString())
                : new StoredEffect(
                    e.Id.ToEffectId(),
                    WorkStatus.Failed,
                    Result: null,
                    StoredException: Serializer.SerializeException(FatalWorkflowException.CreateNonGeneric(flowId, e.Exception)),
                    Alias: e.Alias
                )
        ).ToList();

    public IReadOnlyList<StoredMessage> MapInitialMessages(IEnumerable<MessageAndIdempotencyKey> initialMessages)
        => initialMessages.Select(m =>
        {
            Serializer.Serialize(m.Message, out var content, out var type);
            return new StoredMessage(content, type, Position: 0, m.IdempotencyKey);
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