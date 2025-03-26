using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal class InvocationHelper<TParam, TReturn> 
{
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly IFunctionStore _functionStore;
    private readonly SettingsWithDefaults _settings;
    private readonly bool _isParamlessFunction;
    private readonly FlowType _flowType;
    private readonly StoredType _storedType;
    private readonly LeasesUpdater _leasesUpdater;
    private readonly ResultBusyWaiter<TReturn> _resultBusyWaiter;

    private ISerializer Serializer { get; }

    public InvocationHelper(FlowType flowType, StoredType storedType, bool isParamlessFunction, SettingsWithDefaults settings, IFunctionStore functionStore, ShutdownCoordinator shutdownCoordinator, LeasesUpdater leasesUpdater, ISerializer serializer)
    {
        _flowType = flowType;
        _isParamlessFunction = isParamlessFunction;
        _settings = settings;

        Serializer = serializer;
        _shutdownCoordinator = shutdownCoordinator;
        _leasesUpdater = leasesUpdater;
        _storedType = storedType;
        _functionStore = functionStore;
        _resultBusyWaiter = new ResultBusyWaiter<TReturn>(_functionStore, Serializer);
    }

    public async Task<Tuple<bool, IDisposable>> PersistFunctionInStore(
        FlowId flowId,
        StoredId storedId, 
        FlowInstance humanInstanceId, 
        TParam param, 
        DateTime? scheduleAt,
        StoredId? parent,
        InitialState? initialState)
    {
        if (!_isParamlessFunction)
            ArgumentNullException.ThrowIfNull(param);
        
        var runningFunction = _shutdownCoordinator.RegisterRunningFunction();
        try
        {
            var storedParameter = SerializeParameter(param);
            var utcNowTicks = DateTime.UtcNow.Ticks;
            var effects = initialState == null
                ? null
                : MapInitialEffects(initialState.Effects, flowId);
            var messages = initialState == null
                ? null
                : MapInitialMessages(initialState.Messages);
            
            var created = await _functionStore.CreateFunction(
                storedId,
                humanInstanceId,
                storedParameter,
                leaseExpiration: utcNowTicks + _settings.LeaseLength.Ticks,
                postponeUntil: scheduleAt?.ToUniversalTime().Ticks,
                timestamp: utcNowTicks,
                parent: parent,
                effects,
                messages
            );

            if (!created) runningFunction.Dispose();
            return Tuple.Create(created, runningFunction);
        }
        catch (Exception)
        {
            runningFunction.Dispose();
            throw;
        }
    }

    public async Task<TReturn> WaitForFunctionResult(FlowId flowId, StoredId storedId, bool allowPostponedAndSuspended, TimeSpan? maxWait)
        => await _resultBusyWaiter.WaitForFunctionResult(flowId, storedId, allowPostponedAndSuspended, maxWait);
    
    public async Task PersistFailure(StoredId storedId, FlowId flowId, FatalWorkflowException exception, TParam param, StoredId? parent, int expectedEpoch)
    {
        var storedException = Serializer.SerializeException(exception);
        
        var success = await _functionStore.FailFunction(
            storedId,
            storedException,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch,
            complimentaryState: new ComplimentaryState(
                () => SerializeParameter(param),
                _settings.LeaseLength.Ticks
            )
        );
        if (!success) 
            throw UnexpectedStateException.ConcurrentModification(storedId);
    }

    public async Task<PersistResultOutcome> PersistResult(
        StoredId storedId,
        FlowId flowId,
        Result<TReturn> result,
        TParam param,
        StoredId? parent,
        int expectedEpoch)
    {
        var complementaryState = new ComplimentaryState(
            () => SerializeParameter(param),
            _settings.LeaseLength.Ticks
        );
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return await _functionStore.SucceedFunction(
                    storedId,
                    result: SerializeResult(result.SucceedWithValue),
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Postpone:
                return await _functionStore.PostponeFunction(
                    storedId,
                    postponeUntil: result.Postpone!.DateTime.Ticks,
                    timestamp: DateTime.UtcNow.Ticks,
                    ignoreInterrupted: false, 
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Reschedule;
            case Outcome.Fail:
                return await _functionStore.FailFunction(
                    storedId,
                    storedException: Serializer.SerializeException(result.Fail!),
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Suspend:
                return await _functionStore.SuspendFunction(
                    storedId,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
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
        
        var (content, type) = Serializer.SerializeMessage(msg, msg.GetType());
        var storedMessage = new StoredMessage(content, type, IdempotencyKey: $"FlowCompleted:{childId}");
        await _functionStore.MessageStore.AppendMessage(parent, storedMessage);
        await _functionStore.Interrupt(parent, onlyIfExecuting: false);
    }

    public async Task<RestartedFunction?> RestartFunction(StoredId flowId, int expectedEpoch)
    {
        var runningFunction = _shutdownCoordinator.RegisterRunningFunction();

        try
        {
            var sf = await _functionStore.RestartExecution(
                flowId,
                expectedEpoch,
                leaseExpiration: DateTime.UtcNow.Ticks + _settings.LeaseLength.Ticks
            );

            return sf != null
                ? new RestartedFunction(sf.StoredFlow, runningFunction) //todo extend this class as well
                : null;
        }
        catch
        {
            runningFunction.Dispose();
            throw;
        }
    }
    
    
    public async Task<PreparedReInvocation> PrepareForReInvocation(StoredId storedId, RestartedFunction restartedFunction)
    {
        var (sf, runningFunction) = restartedFunction;
        var expectedEpoch = sf.Epoch;
        var flowId = new FlowId(_flowType, sf.HumanInstanceId);
        
        try
        {
            var param = sf.Parameter == null 
                ? default 
                : Serializer.Deserialize<TParam>(sf.Parameter);                
            
            return new PreparedReInvocation(flowId, param, sf.Epoch, runningFunction, sf.ParentId);
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
                timestamp: DateTime.UtcNow.Ticks,
                expectedEpoch,
                complimentaryState: new ComplimentaryState(
                    () => sf.Parameter,
                    _settings.LeaseLength.Ticks
                )
            );
            throw;
        }
        catch (Exception)
        {
            runningFunction.Dispose();
            throw;
        }
    }

    internal record PreparedReInvocation(FlowId FlowId, TParam? Param, int Epoch, IDisposable RunningFunction, StoredId? Parent);

    public IDisposable StartLeaseUpdater(StoredId storedId, int epoch = 0) 
        => LeaseUpdater.CreateAndStart(storedId, epoch, _leasesUpdater);
    
    public async Task<bool> SetFunctionState(
        StoredId storedId,
        Status status,
        TParam param,
        TReturn? result,
        long expires,
        FatalWorkflowException? exception,
        int expectedEpoch
    )
    {
        return await _functionStore.SetFunctionState(
            storedId,
            status,
            param: SerializeParameter(param),
            result: SerializeResult(result),
            exception == null ? null : Serializer.SerializeException(exception),
            expires,
            expectedEpoch
        );
    }

    public async Task<bool> SaveControlPanelChanges(
        StoredId storedId, 
        TParam param, 
        TReturn? @return,
        int expectedEpoch)
    {
        return await _functionStore.SetParameters(
            storedId,
            param: SerializeParameter(param),
            result: SerializeResult(@return),
            expectedEpoch
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

        return new FunctionState<TParam, TReturn>(
            sf.Status,
            sf.Epoch,
            sf.Expires,
            Param:
                sf.Parameter == null 
                ? default
                : Serializer.Deserialize<TParam>(sf.Parameter),
            Result: sf.Result == null 
                ? default 
                : Serializer.Deserialize<TReturn>(sf.Result),
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
            var marked = await parent.Effect.Mark($"BulkScheduled#{parent.Effect.TakeNextImplicitId()}");
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
                    new StoredId(_storedType, bw.Instance.ToStoredInstance()),
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

    public Messages CreateMessages(StoredId storedId, ScheduleReInvocation scheduleReInvocation, Func<bool> isWorkflowRunning, Effect effect)
    {
        var messageWriter = new MessageWriter(storedId, _functionStore, Serializer, scheduleReInvocation);
        var registeredTimeouts = new RegisteredTimeouts(storedId, _functionStore.TimeoutStore, effect);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: _settings.MessagesPullFrequency,
            _settings.MessagesDefaultMaxWaitForCompletion,
            isWorkflowRunning,
            _functionStore,
            Serializer,
            registeredTimeouts
        );
        
        return new Messages(messageWriter, registeredTimeouts, messagesPullerAndEmitter);
    }
    
    private static Task<IReadOnlyList<StoredEffect>> EmptyList { get; } = Task.FromResult((IReadOnlyList<StoredEffect>) new List<StoredEffect>());
    public Tuple<Effect, States> CreateEffectAndStates(StoredId storedId, FlowId flowId, bool anyEffects)
    {
        var effectsStore = _functionStore.EffectsStore;
        
        var lazyEffects = !anyEffects 
            ? new Lazy<Task<IReadOnlyList<StoredEffect>>>(EmptyList)
            : new Lazy<Task<IReadOnlyList<StoredEffect>>>(() => effectsStore.GetEffectResults(storedId));
        
        var states = new States(
            storedId,
            effectsStore,
            lazyEffects,
            Serializer
        );

        var effectResults = new EffectResults(
            flowId,
            storedId,
            lazyEffects,
            effectsStore,
            Serializer
        );
        
        var effect = new Effect(effectResults);
       return Tuple.Create(effect, states);
    }
    
    public Correlations CreateCorrelations(FlowId flowId)
    {
        var correlationStore = _functionStore.CorrelationStore;
        return new Correlations(MapToStoredId(flowId), correlationStore);
    }

    public ExistingStates CreateExistingStates(FlowId flowId)
        => new(
            MapToStoredId(flowId),
            _functionStore,
            Serializer
        );

    public ExistingEffects CreateExistingEffects(FlowId flowId) => new(MapToStoredId(flowId), flowId, _functionStore.EffectsStore, Serializer);
    public ExistingMessages CreateExistingMessages(FlowId flowId) => new(MapToStoredId(flowId), _functionStore.MessageStore, Serializer);
    public ExistingRegisteredTimeouts CreateExistingTimeouts(FlowId flowId, ExistingEffects existingEffects) => new(MapToStoredId(flowId), _functionStore.TimeoutStore, existingEffects);
    public ExistingSemaphores CreateExistingSemaphores(FlowId flowId) => new(MapToStoredId(flowId), _functionStore, CreateExistingEffects(flowId));

    public DistributedSemaphores CreateSemaphores(StoredId storedId, Effect effect)
        => new(effect, _functionStore.SemaphoreStore, storedId, Interrupt);
    
    public StoredId MapToStoredId(FlowId flowId) => new(_storedType, flowId.Instance.ToStoredInstance());
    
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
    
    private IReadOnlyList<StoredEffect> MapInitialEffects(IEnumerable<InitialEffect> initialEffects, FlowId flowId)
    => initialEffects
        .Select(e =>
            e.Exception == null
                ? new StoredEffect(
                    e.Id.ToEffectId(EffectType.Effect),
                    e.Id.ToEffectId(EffectType.Effect).ToStoredEffectId(),
                    e.Status ?? WorkStatus.Completed,
                    Result: Serializer.Serialize(e.Value, e.Value?.GetType() ?? typeof(object)),
                    StoredException: null)
                : new StoredEffect(
                    e.Id.ToEffectId(EffectType.Effect),
                    e.Id.ToEffectId(EffectType.Effect).ToStoredEffectId(),
                    WorkStatus.Failed,
                    Result: null,
                    StoredException: Serializer.SerializeException(FatalWorkflowException.CreateNonGeneric(flowId, e.Exception))
                )
        ).ToList();

    private IReadOnlyList<StoredMessage> MapInitialMessages(IEnumerable<MessageAndIdempotencyKey> initialMessages)
        => initialMessages.Select(m =>
        {
            var (content, type) = Serializer.SerializeMessage(m.Message, m.Message.GetType());
            return new StoredMessage(content, type, m.IdempotencyKey);
        }).ToList();
}