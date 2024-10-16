﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
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

    private ISerializer Serializer { get; }

    public InvocationHelper(FlowType flowType, bool isParamlessFunction, SettingsWithDefaults settings, IFunctionStore functionStore, ShutdownCoordinator shutdownCoordinator)
    {
        _flowType = flowType;
        _isParamlessFunction = isParamlessFunction;
        _settings = settings;

        Serializer = new ErrorHandlingDecorator(settings.Serializer);
        _shutdownCoordinator = shutdownCoordinator;
        _functionStore = functionStore;
    }

    public async Task<Tuple<bool, IDisposable>> PersistFunctionInStore(
        FlowId flowId, 
        TParam param, 
        DateTime? scheduleAt)
    {
        if (!_isParamlessFunction)
            ArgumentNullException.ThrowIfNull(param);
        
        var runningFunction = _shutdownCoordinator.RegisterRunningFunction();
        try
        {
            var storedParameter = SerializeParameter(param);

            var utcNowTicks = DateTime.UtcNow.Ticks;
            var created = await _functionStore.CreateFunction(
                flowId,
                storedParameter,
                postponeUntil: scheduleAt?.ToUniversalTime().Ticks,
                leaseExpiration: utcNowTicks + _settings.LeaseLength.Ticks,
                timestamp: utcNowTicks
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
    
    public async Task<TReturn> WaitForFunctionResult(FlowId flowId, bool allowPostponedAndSuspended) 
    {
        while (true)
        {
            var storedFunction = await _functionStore.GetFunction(flowId);
            if (storedFunction == null)
                throw UnexpectedStateException.NotFound(flowId);

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
                    throw new PreviousInvocationException(flowId, error);
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
    
    public async Task PersistFailure(FlowId flowId, Exception exception, TParam param, string? defaultState, int expectedEpoch)
    {
        var serializer = _settings.Serializer;
        var storedException = serializer.SerializeException(exception);

        var success = await _functionStore.FailFunction(
            flowId,
            storedException,
            defaultState,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch,
            complimentaryState: new ComplimentaryState(
                () => SerializeParameter(param),
                _settings.LeaseLength.Ticks
            )
        );
        if (!success) 
            throw UnexpectedStateException.ConcurrentModification(flowId);
    }

    public async Task<PersistResultOutcome> PersistResult(
        FlowId flowId,
        Result<TReturn> result,
        TParam param,
        string? defaultState,
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
                    flowId,
                    result: SerializeResult(result.SucceedWithValue),
                    defaultState,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Postpone:
                return await _functionStore.PostponeFunction(
                    flowId,
                    postponeUntil: result.Postpone!.DateTime.Ticks,
                    defaultState,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Fail:
                return await _functionStore.FailFunction(
                    flowId,
                    storedException: Serializer.SerializeException(result.Fail!),
                    defaultState,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Suspend:
                var success = await _functionStore.SuspendFunction(
                    flowId,
                    defaultState,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
                if (success) return PersistResultOutcome.Success;
                success = await _functionStore.PostponeFunction(
                    flowId,
                    postponeUntil: DateTime.UtcNow.Add(_settings.LeaseLength).Ticks,
                    defaultState,
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

    public async Task<RestartedFunction?> RestartFunction(FlowId flowId, int expectedEpoch)
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
    
    
    public async Task<PreparedReInvocation> PrepareForReInvocation(FlowId flowId, RestartedFunction restartedFunction)
    {
        var (sf, runningFunction) = restartedFunction;
        var expectedEpoch = sf.Epoch;
        
        try
        {
            var param = sf.Parameter == null 
                ? default 
                : Serializer.DeserializeParameter<TParam>(sf.Parameter);                
            
            return new PreparedReInvocation(param, sf.Epoch, sf.DefaultState, runningFunction);
        }
        catch (DeserializationException e)
        {
            runningFunction.Dispose();
            sf = await _functionStore.GetFunction(flowId);
            if (sf == null)
                throw UnexpectedStateException.NotFound(flowId);
            
            await _functionStore.FailFunction(
                flowId,
                storedException: Serializer.SerializeException(e),
                sf.DefaultState,
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

    internal record PreparedReInvocation(TParam? Param, int Epoch, string? DefaultState, IDisposable RunningFunction);

    public IDisposable StartLeaseUpdater(FlowId flowId, int epoch = 0) 
        => LeaseUpdater.CreateAndStart(flowId, epoch, _functionStore, _settings);
    
    public async Task<bool> SetFunctionState(
        FlowId flowId,
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
            flowId,
            status,
            param: SerializeParameter(param),
            result: SerializeResult(result),
            exception == null ? null : serializer.SerializeException(exception),
            expires,
            expectedEpoch
        );
    }

    public async Task<bool> SaveControlPanelChanges(
        FlowId flowId, 
        TParam param, 
        TReturn? @return,
        int expectedEpoch)
    {
        return await _functionStore.SetParameters(
            flowId,
            param: SerializeParameter(param),
            result: SerializeResult(@return),
            expectedEpoch
        );
    }

    public async Task Delete(FlowId flowId)
    {
        await _functionStore.DeleteFunction(flowId);
    }
    
    public async Task<FunctionState<TParam, TReturn>?> GetFunction(FlowId flowId)
    {
        var serializer = _settings.Serializer;
        
        var sf = await _functionStore.GetFunction(flowId);
        if (sf == null) 
            return null;

        return new FunctionState<TParam, TReturn>(
            flowId,
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
            sf.DefaultState,
            PreviouslyThrownException: sf.Exception == null 
                ? null 
                : serializer.DeserializeException(sf.Exception)
        );
    }

    public async Task BulkSchedule(IEnumerable<BulkWork<TParam>> work)
    {
        var serializer = _settings.Serializer;
        await _functionStore.BulkScheduleFunctions(
            work.Select(bw =>
                new IdWithParam(
                    new FlowId(_flowType, bw.Instance),
                    _isParamlessFunction ? null : serializer.SerializeParameter(bw.Param)
                )
            )
        );
    }

    public Messages CreateMessages(FlowId flowId, ScheduleReInvocation scheduleReInvocation, Func<bool> isWorkflowRunning)
    {
        var messageWriter = new MessageWriter(flowId, _functionStore, Serializer, scheduleReInvocation);
        var registeredTimeouts = new RegisteredTimeouts(flowId, _functionStore.TimeoutStore);
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            flowId,
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
    public Tuple<Effect, States> CreateEffectAndStates(FlowId flowId, string? defaultState, bool anyEffects)
    {
        var effectsStore = _functionStore.EffectsStore;

        var lazyEffects = !anyEffects 
            ? new Lazy<Task<IReadOnlyList<StoredEffect>>>(EmptyList)
            : new Lazy<Task<IReadOnlyList<StoredEffect>>>(() => effectsStore.GetEffectResults(flowId));
        
        var states = new States(
            flowId,
            defaultState,
            _functionStore,
            effectsStore,
            lazyEffects,
            _settings.Serializer
        );
        
        var effect = new Effect(
            flowId,
            lazyEffects,
            effectsStore,
            _settings.Serializer
        );

       return Tuple.Create(effect, states);
    }
    
    public Correlations CreateCorrelations(FlowId flowId)
    {
        var correlationStore = _functionStore.CorrelationStore;
        return new Correlations(flowId, correlationStore);
    }

    public ExistingStates CreateExistingStates(FlowId flowId, string? defaultState)
        => new(
            flowId,
            defaultState,
            _functionStore,
            _settings.Serializer
        );

    public ExistingEffects CreateExistingEffects(FlowId flowId) => new(flowId, _functionStore.EffectsStore, _settings.Serializer);
    public ExistingMessages CreateExistingMessages(FlowId flowId) => new(flowId, _functionStore.MessageStore, _settings.Serializer);
    public ExistingRegisteredTimeouts CreateExistingTimeouts(FlowId flowId) => new(flowId, _functionStore.TimeoutStore);

    private string? SerializeParameter(TParam param)
    {
        if (typeof(TParam) == typeof(Unit))
            return null;
        
        return param is null
            ? null 
            : Serializer.SerializeParameter(param);
    }
    
    private string? SerializeResult(TReturn? result)
    {
        if (typeof(TReturn) == typeof(Unit))
            return null;
        
        return result is null
            ? null 
            : Serializer.SerializeResult(result);
    }
}