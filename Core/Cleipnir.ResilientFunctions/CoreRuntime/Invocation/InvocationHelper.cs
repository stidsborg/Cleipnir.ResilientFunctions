using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Reactive.Extensions;
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

    private ISerializer Serializer { get; }

    public InvocationHelper(FlowType flowType, StoredType storedType, bool isParamlessFunction, SettingsWithDefaults settings, IFunctionStore functionStore, ShutdownCoordinator shutdownCoordinator)
    {
        _flowType = flowType;
        _isParamlessFunction = isParamlessFunction;
        _settings = settings;

        Serializer = new ErrorHandlingDecorator(settings.Serializer);
        _shutdownCoordinator = shutdownCoordinator;
        _storedType = storedType;
        _functionStore = functionStore;
    }

    public async Task<Tuple<bool, IDisposable>> PersistFunctionInStore(
        StoredId storedId, 
        FlowInstance humanInstanceId, 
        TParam param, 
        DateTime? scheduleAt,
        StoredId? parent)
    {
        if (!_isParamlessFunction)
            ArgumentNullException.ThrowIfNull(param);
        
        var runningFunction = _shutdownCoordinator.RegisterRunningFunction();
        try
        {
            var storedParameter = SerializeParameter(param);

            var utcNowTicks = DateTime.UtcNow.Ticks;
            var created = await _functionStore.CreateFunction(
                storedId,
                humanInstanceId,
                storedParameter,
                postponeUntil: scheduleAt?.ToUniversalTime().Ticks,
                leaseExpiration: utcNowTicks + _settings.LeaseLength.Ticks,
                timestamp: utcNowTicks,
                parent: parent
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
    {
        var stopWatch = Stopwatch.StartNew();
        while (true)
        {
            if (maxWait.HasValue && stopWatch.Elapsed > maxWait.Value)
                throw new TimeoutException();
            
            var storedFunction = await _functionStore.GetFunction(storedId);
            if (storedFunction == null)
                throw UnexpectedStateException.NotFound(storedId);

            switch (storedFunction.Status)
            {
                case Status.Executing:
                    await Task.Delay(250);
                    continue;
                case Status.Succeeded:
                    return 
                        storedFunction.Result == null 
                            ? default!
                            : _settings.Serializer.DeserializeResult<TReturn>(storedFunction.Result);
                case Status.Failed:
                    var error = Serializer.DeserializeException(storedFunction.Exception!);
                    throw new PreviousInvocationException(_flowType, error);
                case Status.Postponed:
                    if (allowPostponedAndSuspended) { await Task.Delay(250); continue;}
                    throw new InvocationPostponedException(
                        flowId,
                        postponedUntil: new DateTime(storedFunction.Expires, DateTimeKind.Utc)
                    );
                case Status.Suspended:
                    if (allowPostponedAndSuspended) { await Task.Delay(250); continue; }
                    throw new InvocationSuspendedException(flowId);
                default:
                    throw new ArgumentOutOfRangeException(); 
            }
        }
    }
    
    public async Task PersistFailure(StoredId storedId, FlowId flowId, Exception exception, TParam param, StoredId? parent, int expectedEpoch)
    {
        var serializer = _settings.Serializer;
        var storedException = serializer.SerializeException(exception);
        
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
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Fail:
                return await _functionStore.FailFunction(
                    storedId,
                    storedException: Serializer.SerializeException(result.Fail!),
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Suspend:
                var success = await _functionStore.SuspendFunction(
                    storedId,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
                if (success) return PersistResultOutcome.Success;
                success = await _functionStore.PostponeFunction(
                    storedId,
                    postponeUntil: DateTime.UtcNow.Add(_settings.LeaseLength).Ticks,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
                return success 
                    ? PersistResultOutcome.Reschedule 
                    : PersistResultOutcome.Failed;
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
                throw result.Fail!;
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

        var serializer = _settings.Serializer;
        var msg = result.Outcome switch
        {
            Outcome.Succeed => new FlowCompleted(childId, Result: SerializeResult(result.SucceedWithValue), Failed: false),
            Outcome.Fail => new FlowCompleted(childId, Result: null, Failed: true),
            _ => default
        };

        if (msg == null)
            return;
        
        var (content, type) = serializer.SerializeMessage(msg);
        var storedMessage = new StoredMessage(content, type, IdempotencyKey: $"FlowCompleted:{parent}");
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
                ? new RestartedFunction(sf, runningFunction)
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
                : Serializer.DeserializeParameter<TParam>(sf.Parameter);                
            
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
                storedException: Serializer.SerializeException(e),
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

    public IDisposable StartLeaseUpdater(StoredId storedId, FlowId flowId, int epoch = 0) 
        => LeaseUpdater.CreateAndStart(storedId, flowId, epoch, _functionStore, _settings);
    
    public async Task<bool> SetFunctionState(
        StoredId storedId,
        Status status,
        TParam param,
        TReturn? result,
        long expires,
        Exception? exception,
        int expectedEpoch
    )
    {
        var serializer = _settings.Serializer;
        return await _functionStore.SetFunctionState(
            storedId,
            status,
            param: SerializeParameter(param),
            result: SerializeResult(result),
            exception == null ? null : serializer.SerializeException(exception),
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

    public async Task Delete(StoredId storedId)
    {
        await _functionStore.DeleteFunction(storedId);
    }
    
    public async Task<FunctionState<TParam, TReturn>?> GetFunction(StoredId storedId)
    {
        var serializer = _settings.Serializer;
        
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
                : serializer.DeserializeParameter<TParam>(sf.Parameter),
            Result: sf.Result == null 
                ? default 
                : serializer.DeserializeResult<TReturn>(sf.Result),
            PreviouslyThrownException: sf.Exception == null 
                ? null 
                : serializer.DeserializeException(sf.Exception)
        );
    }

    public async Task<InnerScheduled<TReturn>> BulkSchedule(IEnumerable<BulkWork<TParam>> work, bool? detach = null)
    {
        var serializer = _settings.Serializer;
        var parent = GetAndEnsureParent(detach);
        
        await _functionStore.BulkScheduleFunctions(
            work.Select(bw =>
                new IdWithParam(
                    new StoredId(_storedType, bw.Instance),
                    bw.Instance,
                    _isParamlessFunction ? null : serializer.SerializeParameter(bw.Param)
                )
            ),
            parent?.StoredId
        );

        if (detach == false || parent == null)
            return InnerScheduled<TReturn>.Failing;

        return CreateInnerScheduled(
            work.Select(w => new FlowId(_flowType, w.Instance)).ToList(),
            parent,
            detach
        );
    }

    public Messages CreateMessages(StoredId storedId, ScheduleReInvocation scheduleReInvocation, Func<bool> isWorkflowRunning)
    {
        var messageWriter = new MessageWriter(storedId, _functionStore, Serializer, scheduleReInvocation);
        var registeredTimeouts = new RegisteredTimeouts(storedId, _functionStore.TimeoutStore);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            storedId,
            defaultDelay: _settings.MessagesPullFrequency,
            _settings.MessagesDefaultMaxWaitForCompletion,
            isWorkflowRunning,
            _functionStore,
            _settings.Serializer,
            registeredTimeouts
        );
        
        return new Messages(messageWriter, registeredTimeouts, messagesPullerAndEmitter);
    }
    
    private static Task<IReadOnlyList<StoredEffect>> EmptyList { get; }
        = Task.FromResult((IReadOnlyList<StoredEffect>) new List<StoredEffect>());
    public Tuple<Effect, States> CreateEffectAndStates(StoredId storedId, bool anyEffects)
    {
        var effectsStore = _functionStore.EffectsStore;
        
        var lazyEffects = !anyEffects 
            ? new Lazy<Task<IReadOnlyList<StoredEffect>>>(EmptyList)
            : new Lazy<Task<IReadOnlyList<StoredEffect>>>(() => effectsStore.GetEffectResults(storedId));
        
        var states = new States(
            storedId,
            _functionStore,
            effectsStore,
            lazyEffects,
            _settings.Serializer
        );
        
        var effect = new Effect(
            _flowType,
            storedId,
            lazyEffects,
            effectsStore,
            _settings.Serializer
        );

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
            _settings.Serializer
        );

    public ExistingEffects CreateExistingEffects(FlowId flowId) => new(MapToStoredId(flowId), _functionStore.EffectsStore, _settings.Serializer);
    public ExistingMessages CreateExistingMessages(FlowId flowId) => new(MapToStoredId(flowId), _functionStore.MessageStore, _settings.Serializer);
    public ExistingRegisteredTimeouts CreateExistingTimeouts(FlowId flowId) => new(MapToStoredId(flowId), _functionStore.TimeoutStore);

    public StoredId MapToStoredId(FlowId flowId) => new(_storedType, flowId.Instance.Value);
    
    private byte[]? SerializeParameter(TParam param)
    {
        if (typeof(TParam) == typeof(Unit))
            return null;
        
        return param is null
            ? null 
            : Serializer.SerializeParameter(param);
    }
    
    private byte[]? SerializeResult(TReturn? result)
    {
        if (typeof(TReturn) == typeof(Unit))
            return null;
        
        return result is null
            ? null 
            : Serializer.SerializeResult(result);
    }

    public InnerScheduled<TReturn> CreateInnerScheduled(List<FlowId> scheduledIds, Workflow? parentWorkflow, bool? detach)
    {
        if (detach == false || parentWorkflow == null)
            return new InnerScheduled<TReturn>(async maxWait =>
            {
                //todo make max wait smaller after each await
                var results = new List<TReturn>(scheduledIds.Count);
                foreach (var scheduledId in scheduledIds)
                {
                    var result = await WaitForFunctionResult(scheduledId, scheduledId.ToStoredId(_storedType), allowPostponedAndSuspended: true, maxWait);
                    results.Add(result);
                }

                return results;
            });
        
        return new InnerScheduled<TReturn>(async maxWait =>
        {
            var completedFlows = await parentWorkflow!
                .Messages
                .OfType<FlowCompleted>()
                .Where(fc => scheduledIds.Contains(fc.Id))
                .Take(scheduledIds.Count)
                .Completion(maxWait);

            var failed = completedFlows.FirstOrDefault(fc => fc.Failed);
            if (failed != null)
                throw new InvalidOperationException($"Child-flow '{failed.Id}' failed");

            var serializer = _settings.Serializer;
            var results = completedFlows.Select(fc =>
                fc.Result == null
                    ? default!
                    : serializer.DeserializeResult<TReturn>(fc.Result)
            );

            return results.ToList();
        });
    }
    
    public Workflow? GetAndEnsureParent(bool? detach)
    {
        if (detach == true)
            return null;
        
        var parentWorkflow = CurrentFlow.Workflow;
        if (parentWorkflow == null && detach == false) 
            throw new InvalidOperationException("Cannot start an attached flow without a parent");
        
        return parentWorkflow;
    }
}