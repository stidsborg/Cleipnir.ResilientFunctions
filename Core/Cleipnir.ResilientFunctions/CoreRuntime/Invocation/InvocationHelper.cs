using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal class InvocationHelper<TParam, TReturn> 
{
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly IFunctionStore _functionStore;
    private readonly SettingsWithDefaults _settings;
    private readonly Func<FunctionId, MessageWriter?> _messageWriterFunc;

    private ISerializer Serializer { get; }

    public InvocationHelper(
        SettingsWithDefaults settings,
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator, 
        Func<FunctionId, MessageWriter> messageWriterFunc)
    {
        _settings = settings;

        Serializer = new ErrorHandlingDecorator(settings.Serializer);
        _shutdownCoordinator = shutdownCoordinator;
        _messageWriterFunc = messageWriterFunc;
        _functionStore = functionStore;
    }

    public async Task<Tuple<bool, IDisposable>> PersistFunctionInStore(
        FunctionId functionId, 
        TParam param, 
        DateTime? scheduleAt)
    {
        ArgumentNullException.ThrowIfNull(param);
        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            var storedParameter = Serializer.SerializeParameter(param);

            var utcNowTicks = DateTime.UtcNow.Ticks;
            var created = await _functionStore.CreateFunction(
                functionId,
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
    
    public async Task<TReturn> WaitForFunctionResult(FunctionId functionId, bool allowPostponedAndSuspended) 
    {
        while (true)
        {
            var storedFunction = await _functionStore.GetFunction(functionId);
            if (storedFunction == null)
                throw new FrameworkException(functionId.TypeId, $"Function {functionId} does not exist");

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
                    throw new PreviousFunctionInvocationException(functionId, error);
                case Status.Postponed:
                    if (allowPostponedAndSuspended) { await Task.Delay(250); continue;}
                    throw new FunctionInvocationPostponedException(
                        functionId,
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc)
                    );
                case Status.Suspended:
                    if (allowPostponedAndSuspended) { await Task.Delay(250); continue; }
                    throw new FunctionInvocationSuspendedException(functionId);
                default:
                    throw new ArgumentOutOfRangeException(); 
            }
        }
    }
    
    public async Task PersistFailure(FunctionId functionId, Exception exception, TParam param, string? defaultState, int expectedEpoch)
    {
        var serializer = _settings.Serializer;
        var storedException = serializer.SerializeException(exception);

        var success = await _functionStore.FailFunction(
            functionId,
            storedException,
            defaultState,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch,
            complimentaryState: new ComplimentaryState(
                () => serializer.SerializeParameter(param),
                _settings.LeaseLength.Ticks
            )
        );
        if (!success) 
            throw new ConcurrentModificationException(functionId);
    }

    public async Task<PersistResultOutcome> PersistResult(
        FunctionId functionId,
        Result<TReturn> result,
        TParam param,
        string? defaultState,
        int expectedEpoch)
    {
        var complementaryState = new ComplimentaryState(
            () => Serializer.SerializeParameter(param),
            _settings.LeaseLength.Ticks
        );
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return await _functionStore.SucceedFunction(
                    functionId,
                    result: result.SucceedWithValue == null 
                        ? null 
                        : Serializer.SerializeResult(result.SucceedWithValue),
                    defaultState,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Postpone:
                return await _functionStore.PostponeFunction(
                    functionId,
                    postponeUntil: result.Postpone!.DateTime.Ticks,
                    defaultState,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Fail:
                return await _functionStore.FailFunction(
                    functionId,
                    storedException: Serializer.SerializeException(result.Fail!),
                    defaultState,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                ) ? PersistResultOutcome.Success : PersistResultOutcome.Failed;
            case Outcome.Suspend:
                var success = await _functionStore.SuspendFunction(
                    functionId,
                    result.Suspend!.InterruptCount,
                    defaultState,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
                if (success) return PersistResultOutcome.Success;
                success = await _functionStore.PostponeFunction(
                    functionId,
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

    public static void EnsureSuccess(FunctionId functionId, Result<TReturn> result, bool allowPostponedOrSuspended)
    {
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return;
            case Outcome.Postpone:
                if (allowPostponedOrSuspended)
                    return;
                else
                    throw new FunctionInvocationPostponedException(functionId, result.Postpone!.DateTime);
            case Outcome.Fail:
                throw result.Fail!;
            case Outcome.Suspend:
                if (allowPostponedOrSuspended)
                    return;
                else
                    throw new FunctionInvocationSuspendedException(functionId);
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public async Task<PreparedReInvocation> PrepareForReInvocation(FunctionId functionId, int expectedEpoch) 
    {
        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            var sf = await _functionStore.RestartExecution(
                functionId,
                expectedEpoch,
                leaseExpiration: DateTime.UtcNow.Ticks + _settings.LeaseLength.Ticks 
            );
            if (sf == null)
                throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' did not have expected epoch: '{expectedEpoch}'");

            expectedEpoch = sf.Epoch;

            var param = sf.Parameter == null 
                ? default 
                : Serializer.DeserializeParameter<TParam>(sf.Parameter);                
            
            return new PreparedReInvocation(param, sf.Epoch, sf.DefaultState, runningFunction);
        }
        catch (DeserializationException e)
        {
            runningFunction.Dispose();
            var sf = await _functionStore.GetFunction(functionId);
            if (sf == null)
                throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' was not found");
            
            await _functionStore.FailFunction(
                functionId,
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

    public IDisposable StartLeaseUpdater(FunctionId functionId, int epoch = 0) 
        => LeaseUpdater.CreateAndStart(functionId, epoch, _functionStore, _settings);

    public async Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        TParam param,
        DateTime? postponeUntil,
        Exception? exception,
        int expectedEpoch
    )
    {
        var serializer = _settings.Serializer;
        return await _functionStore.SetFunctionState(
            functionId,
            status,
            param: serializer.SerializeParameter(param),
            result: null,
            exception == null ? null : serializer.SerializeException(exception),
            postponeUntil?.Ticks,
            expectedEpoch
        );
    }
    
    public async Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        TParam param,
        TReturn? result,
        DateTime? postponeUntil,
        Exception? exception,
        int expectedEpoch
    )
    {
        var serializer = _settings.Serializer;
        return await _functionStore.SetFunctionState(
            functionId,
            status,
            param: serializer.SerializeParameter(param),
            result: result == null ? null : serializer.SerializeResult(result),
            exception == null ? null : serializer.SerializeException(exception),
            postponeUntil?.Ticks,
            expectedEpoch
        );
    }

    public async Task<bool> SaveControlPanelChanges(
        FunctionId functionId, 
        TParam param, 
        TReturn? @return,
        int expectedEpoch)
    {
        var serializer = _settings.Serializer;
        return await _functionStore.SetParameters(
            functionId,
            param: serializer.SerializeParameter(param),
            result: serializer.SerializeResult(@return),
            expectedEpoch
        );
    }

    public async Task Delete(FunctionId functionId)
    {
        await _functionStore.DeleteFunction(functionId);
    }
    
    public async Task<FunctionState<TParam, TReturn>?> GetFunction(FunctionId functionId)
    {
        var serializer = _settings.Serializer;
        
        var sf = await _functionStore.GetFunction(functionId);
        if (sf == null) 
            return null;

        return new FunctionState<TParam, TReturn>(
            functionId,
            sf.Status,
            sf.Epoch,
            sf.LeaseExpiration,
            Param:
                sf.Parameter == null 
                ? default
                : serializer.DeserializeParameter<TParam>(sf.Parameter),
            Result: sf.Result == null 
                ? default 
                : serializer.DeserializeResult<TReturn>(sf.Result),
            sf.DefaultState,
            PostponedUntil: sf.PostponedUntil == null ? null : new DateTime(sf.PostponedUntil.Value),
            PreviouslyThrownException: sf.Exception == null 
                ? null 
                : serializer.DeserializeException(sf.Exception)
        );
    }

    public async Task<Messages> CreateMessages(FunctionId functionId, ScheduleReInvocation scheduleReInvocation, Func<bool> isWorkflowRunning, bool sync)
    {
        var messageWriter = new MessageWriter(functionId, _functionStore, Serializer, scheduleReInvocation);
        var timeoutProvider = new TimeoutProvider(
            functionId,
            _functionStore.TimeoutStore,
            messageWriter,
            _settings.TimeoutEventsCheckFrequency
        );
        var messagesPullerAndEmitter = new MessagesPullerAndEmitter(
            functionId,
            _settings.MessagesPullFrequency,
            isWorkflowRunning,
            _functionStore,
            _settings.Serializer,
            timeoutProvider
        );
        var messages = new Messages(messageWriter, timeoutProvider, messagesPullerAndEmitter);

        if (sync)
            await Task.WhenAll(
                timeoutProvider.PendingTimeouts(), //syncs local timeout cache inside the timeout provider
                messages.Sync()
            );
        
        return messages;
    }

    public async Task<Effect> CreateEffect(FunctionId functionId, bool sync)
    {
        var effectsStore = _functionStore.EffectsStore;
        var existingActivities = sync 
            ? await effectsStore.GetEffectResults(functionId)
            : Enumerable.Empty<StoredEffect>();
        
        return new Effect(functionId, existingActivities, effectsStore, _settings.Serializer);
    }

    public async Task<States> CreateStates(FunctionId functionId, string? defaultState, bool sync)
    {
        var statesStore = _functionStore.StatesStore;
        var serializer = _settings.Serializer;
        if (!sync)
            return new States(
                functionId,
                defaultState,
                existingStates: Enumerable.Empty<StoredState>(),
                _functionStore,
                statesStore,
                serializer
            );

        var existingStoredStates = await statesStore.GetStates(functionId);
        return new States(functionId, defaultState, existingStoredStates, _functionStore, statesStore, serializer);
    }

    public async Task<ExistingStates> GetExistingStates(FunctionId functionId, string? defaultState)
        => new(
            functionId,
            defaultState,
            await _functionStore.StatesStore.GetStates(functionId),
            _functionStore,
            _settings.Serializer
        );
    
    public async Task<ExistingEffects> GetExistingEffects(FunctionId functionId)
    {
        var effectsStore = _functionStore.EffectsStore;
        var existingEffects = await effectsStore.GetEffectResults(functionId);
        return new ExistingEffects(
            functionId,
            existingEffects.ToDictionary(sa => sa.EffectId, sa => sa),
            effectsStore,
            _settings.Serializer
        );
    }

    public async Task<ExistingMessages> GetExistingMessages(FunctionId functionId)
    {
        var storedMessages = await _functionStore.MessageStore.GetMessages(functionId, skip: 0);
        var messages = storedMessages
            .Select(se => new MessageAndIdempotencyKey(
                    _settings.Serializer.DeserializeMessage(se.MessageJson, se.MessageType),
                    se.IdempotencyKey
                )
            ).ToList();
        
        return new ExistingMessages(functionId, messages, _functionStore.MessageStore, _settings.Serializer);
    }

    public async Task<ExistingTimeouts> GetExistingTimeouts(FunctionId functionId)
        => new ExistingTimeouts(
            functionId,
            _functionStore.TimeoutStore,
            await _functionStore.TimeoutStore.GetTimeouts(functionId)
        );
}