using System;
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

internal class InvocationHelper<TParam, TState, TReturn> 
    where TParam : notnull where TState : WorkflowState, new() 
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
        TState state,
        DateTime? scheduleAt)
    {
        ArgumentNullException.ThrowIfNull(param);
        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            var storedParameter = Serializer.SerializeParameter(param);
            var storedState = Serializer.SerializeState(state);

            var utcNowTicks = DateTime.UtcNow.Ticks;
            var created = await _functionStore.CreateFunction(
                functionId,
                storedParameter,
                storedState,
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
                    await Task.Delay(100);
                    continue;
                case Status.Succeeded:
                    return 
                        storedFunction.Result.ResultType == default 
                            ? default! 
                            : storedFunction.Result.Deserialize<TReturn>(Serializer)!;
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

    public void InitializeState(FunctionId functionId, TParam param, TState state, int epoch) 
        => state.Initialize(onSave: () => SaveState(functionId, param, state, epoch, _settings.LeaseLength.Ticks));

    private async Task SaveState(FunctionId functionId, TParam param, TState state, int epoch, long leaseLength)
    {
        var storedParameter = Serializer.SerializeParameter(param);
        var storedState = Serializer.SerializeState(state);
        
        var success = await _functionStore.SaveStateForExecutingFunction(
            functionId,
            storedState.StateJson,
            expectedEpoch: epoch,
            complimentaryState: new ComplimentaryState(() => storedParameter, () => storedState, leaseLength) 
        );

        if (!success)
            throw new StateSaveFailedException(
                functionId,
                $"Unable to save '{functionId}'-state due to concurrent modification"
            );
    }
    
    public async Task PersistFailure(FunctionId functionId, Exception exception, TParam param, TState state, int expectedEpoch)
    {
        var serializer = _settings.Serializer;
        var storedState = serializer.SerializeState(state);
        var storedException = serializer.SerializeException(exception);
        
        var success = await _functionStore.FailFunction(
            functionId,
            storedException,
            storedState.StateJson,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch,
            complimentaryState: new ComplimentaryState(
                () => serializer.SerializeParameter(param), 
                () => storedState, 
                _settings.LeaseLength.Ticks
            )
        );
        if (!success) 
            throw new ConcurrentModificationException(functionId);
    }

    public async Task<bool> PersistResult(
        FunctionId functionId,
        Result<TReturn> result,
        TParam param,
        TState state,
        int expectedEpoch)
    {
        var storedState = Serializer.SerializeState(state);
        var complementaryState = new ComplimentaryState(
            () => Serializer.SerializeParameter(param),
            () => storedState,
            _settings.LeaseLength.Ticks
        );
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return await _functionStore.SucceedFunction(
                    functionId,
                    result: Serializer.SerializeResult(result.SucceedWithValue),
                    stateJson: storedState.StateJson,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
            case Outcome.Postpone:
                return await _functionStore.PostponeFunction(
                    functionId,
                    postponeUntil: result.Postpone!.DateTime.Ticks,
                    stateJson: storedState.StateJson,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
            case Outcome.Fail:
                return await _functionStore.FailFunction(
                    functionId,
                    storedException: Serializer.SerializeException(result.Fail!),
                    stateJson: storedState.StateJson,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
            case Outcome.Suspend:
                return await _functionStore.SuspendFunction(
                    functionId,
                    result.Suspend!.ExpectedMessageCount,
                    storedState.StateJson,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    public async Task PublishFunctionCompletionResult<T>(FunctionId recipient, FunctionId sender, Result<T> result)
    {
        var messageWriter = _messageWriterFunc(recipient);
        if (messageWriter == null)
            throw new InvalidOperationException($"Function '{recipient}' has not been registered and thus function result cannot be published");

        if (typeof(TReturn) == typeof(Unit))
            await messageWriter.AppendMessage(new FunctionCompletion(sender), idempotencyKey: $"FunctionResult¤{sender}");
        else
            await messageWriter.AppendMessage(
                new FunctionCompletion<T>(result.SucceedWithValue!, sender),
                idempotencyKey: $"FunctionResult¤{sender}"
            );
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
            
            var param = Serializer.DeserializeParameter<TParam>(sf.Parameter.ParamJson, sf.Parameter.ParamType);

            var storedState = Serializer.DeserializeState<TState>(
                sf.State.StateJson,
                sf.State.StateType
            );
            storedState.Initialize(onSave: () => SaveState(functionId, param, storedState, sf.Epoch, _settings.LeaseLength.Ticks));
            
            return new PreparedReInvocation(param, sf.Epoch, storedState, runningFunction);
        }
        catch (DeserializationException e)
        {
            runningFunction.Dispose();
            var sf = await _functionStore.GetFunction(functionId);
            await _functionStore.FailFunction(
                functionId,
                storedException: Serializer.SerializeException(e),
                stateJson: sf!.State.StateJson,
                timestamp: DateTime.UtcNow.Ticks,
                expectedEpoch,
                complimentaryState: new ComplimentaryState(
    () => sf.Parameter, 
    () => sf.State, 
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

    internal record PreparedReInvocation(TParam Param, int Epoch, TState State, IDisposable RunningFunction);

    public IDisposable StartLeaseUpdater(FunctionId functionId, int epoch = 0) 
        => LeaseUpdater.CreateAndStart(functionId, epoch, _functionStore, _settings);

    public async Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        TParam param,
        TState state,
        DateTime? postponeUntil,
        Exception? exception,
        int expectedEpoch
    )
    {
        var serializer = _settings.Serializer;
        return await _functionStore.SetFunctionState(
            functionId,
            status,
            storedParameter: serializer.SerializeParameter(param),
            storedState: serializer.SerializeState(state),
            storedResult: StoredResult.Null,
            exception == null ? null : serializer.SerializeException(exception),
            postponeUntil?.Ticks,
            expectedEpoch
        );
    }
    
    public async Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        TParam param,
        TState state,
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
            storedParameter: serializer.SerializeParameter(param),
            storedState: serializer.SerializeState(state),
            storedResult: result == null ? StoredResult.Null : serializer.SerializeResult(result),
            exception == null ? null : serializer.SerializeException(exception),
            postponeUntil?.Ticks,
            expectedEpoch
        );
    }

    public async Task<bool> SaveControlPanelChanges(
        FunctionId functionId, 
        TParam param, 
        TState state,
        TReturn? @return,
        int expectedEpoch)
    {
        var serializer = _settings.Serializer;
        return await _functionStore.SetParameters(
            functionId,
            storedParameter: serializer.SerializeParameter(param),
            storedState: serializer.SerializeState(state),
            storedResult: serializer.SerializeResult(@return),
            expectedEpoch
        );
    }

    public async Task Delete(FunctionId functionId, int expectedEpoch)
    {
        var success = await _functionStore.DeleteFunction(functionId, expectedEpoch);
        
        if (!success)
            throw new ConcurrentModificationException(functionId);
    }
        

    public async Task<FunctionState<TParam, TState, TReturn>?> GetFunction(FunctionId functionId)
    {
        var serializer = _settings.Serializer;
        
        var sf = await _functionStore.GetFunction(functionId);
        if (sf == null) 
            return null;

        return new FunctionState<TParam, TState, TReturn>(
            functionId,
            sf.Status,
            sf.Epoch,
            sf.LeaseExpiration,
            Param: serializer.DeserializeParameter<TParam>(sf.Parameter.ParamJson, sf.Parameter.ParamType),
            State: serializer.DeserializeState<TState>(sf.State.StateJson, sf.State.StateType),
            Result: sf.Result.ResultType == null 
                ? default 
                : serializer.DeserializeResult<TReturn>(sf.Result.ResultJson!, sf.Result.ResultType),
            PostponedUntil: sf.PostponedUntil == null ? null : new DateTime(sf.PostponedUntil.Value),
            PreviouslyThrownException: sf.Exception == null 
                ? null 
                : serializer.DeserializeException(sf.Exception)
        );
    }

    public async Task<Messages> CreateMessages(FunctionId functionId, ScheduleReInvocation scheduleReInvocation, bool sync)
    {
        var messageWriter = new MessageWriter(functionId, _functionStore, Serializer, scheduleReInvocation);
        var timeoutProvider = new TimeoutProvider(functionId, _functionStore.TimeoutStore, messageWriter, _settings.TimeoutEventsCheckFrequency); 
        var messages = new Messages(
            functionId,
            _functionStore.MessageStore,
            messageWriter,
            timeoutProvider,
            _settings.MessagesPullFrequency,
            _settings.Serializer
        );
        
        if (sync)
            await messages.Sync();

        return messages;
    }

    public async Task<Activities> CreateActivities(FunctionId functionId, bool sync)
    {
        var activityStore = _functionStore.ActivityStore;
        var existingActivities = sync 
            ? await activityStore.GetActivityResults(functionId)
            : Enumerable.Empty<StoredActivity>();
        
        return new Activities(functionId, existingActivities, activityStore, _settings.Serializer);
    }

    public async Task<ExistingActivities> GetExistingActivities(FunctionId functionId)
    {
        var activityStore = _functionStore.ActivityStore;
        var existingActivities = await activityStore.GetActivityResults(functionId);
        return new ExistingActivities(
            functionId,
            existingActivities.ToDictionary(sa => sa.ActivityId, sa => sa),
            activityStore,
            _settings.Serializer
        );
    }

    public async Task<ExistingMessages> GetExistingMessages(FunctionId functionId)
    {
        var storedMessages = await _functionStore.MessageStore.GetMessages(functionId);
        var messages = storedMessages
            .Select(se => new MessageAndIdempotencyKey(
                    _settings.Serializer.DeserializeMessage(se.MessageJson, se.MessageType),
                    se.IdempotencyKey
                )
            )
            .Where(m => m.Message is not NoOp)
            .ToList();
        
        return new ExistingMessages(functionId, messages, _functionStore.MessageStore, _settings.Serializer);
    } 

    public ITimeoutProvider CreateTimeoutProvider(FunctionId functionId)
        => new TimeoutProvider(
            functionId,
            _functionStore.TimeoutStore,
            messageWriter: null,
            timeoutCheckFrequency: TimeSpan.Zero
        );
}