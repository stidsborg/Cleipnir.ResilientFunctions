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

internal class InvocationHelper<TParam, TScrapbook, TReturn> 
    where TParam : notnull where TScrapbook : RScrapbook, new() 
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
        TScrapbook scrapbook,
        DateTime? scheduleAt,
        FunctionId? sendResultTo)
    {
        ArgumentNullException.ThrowIfNull(param);
        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            var storedParameter = Serializer.SerializeParameter(param);
            var storedScrapbook = Serializer.SerializeScrapbook(scrapbook);

            var utcNowTicks = DateTime.UtcNow.Ticks;
            var created = await _functionStore.CreateFunction(
                functionId,
                storedParameter,
                storedScrapbook,
                postponeUntil: scheduleAt?.ToUniversalTime().Ticks,
                leaseExpiration: utcNowTicks + _settings.LeaseLength.Ticks,
                timestamp: utcNowTicks,
                sendResultTo: sendResultTo
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
    
    public async Task<TReturn> WaitForFunctionResult(FunctionId functionId, bool allowPostponeAndSuspended) 
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
                    if (allowPostponeAndSuspended) { await Task.Delay(250); continue;}
                    throw new FunctionInvocationPostponedException(
                        functionId,
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc)
                    );
                case Status.Suspended:
                    if (allowPostponeAndSuspended) { await Task.Delay(250); continue; }
                    throw new FunctionInvocationSuspendedException(functionId);
                default:
                    throw new ArgumentOutOfRangeException(); 
            }
        }
    }

    public void InitializeScrapbook(FunctionId functionId, TParam param, TScrapbook scrapbook, int epoch, FunctionId? sendResultTo = null) 
        => scrapbook.Initialize(onSave: () => SaveScrapbook(functionId, param, scrapbook, epoch, _settings.LeaseLength.Ticks, sendResultTo));

    private async Task SaveScrapbook(FunctionId functionId, TParam param, TScrapbook scrapbook, int epoch, long leaseLength, FunctionId? sendResultTo)
    {
        var storedParameter = Serializer.SerializeParameter(param);
        var storedScrapbook = Serializer.SerializeScrapbook(scrapbook);
        
        var success = await _functionStore.SaveScrapbookForExecutingFunction(
            functionId,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: epoch,
            complimentaryState: new ComplimentaryState(() => storedParameter, () => storedScrapbook, leaseLength, sendResultTo) 
        );

        if (!success)
            throw new ScrapbookSaveFailedException(
                functionId,
                $"Unable to save '{functionId}'-scrapbook due to concurrent modification"
            );
    }
    
    public async Task PersistFailure(FunctionId functionId, Exception exception, TParam param, TScrapbook scrapbook, FunctionId? sendResultTo, int expectedEpoch)
    {
        var serializer = _settings.Serializer;
        var storedScrapbook = serializer.SerializeScrapbook(scrapbook);
        var storedException = serializer.SerializeException(exception);
        
        var success = await _functionStore.FailFunction(
            functionId,
            storedException,
            storedScrapbook.ScrapbookJson,
            timestamp: DateTime.UtcNow.Ticks,
            expectedEpoch,
            complimentaryState: new ComplimentaryState(
                () => serializer.SerializeParameter(param), 
                () => storedScrapbook, 
                _settings.LeaseLength.Ticks, 
                sendResultTo
            )
        );
        if (!success) 
            throw new ConcurrentModificationException(functionId);
    }

    public async Task<bool> PersistResult(
        FunctionId functionId,
        Result<TReturn> result,
        TParam param,
        TScrapbook scrapbook,
        FunctionId? sendResultTo,
        int expectedEpoch)
    {
        var storedScrapbook = Serializer.SerializeScrapbook(scrapbook);
        var complementaryState = new ComplimentaryState(
            () => Serializer.SerializeParameter(param),
            () => storedScrapbook,
            _settings.LeaseLength.Ticks, 
            sendResultTo
        );
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                return await _functionStore.SucceedFunction(
                    functionId,
                    result: Serializer.SerializeResult(result.SucceedWithValue),
                    scrapbookJson: storedScrapbook.ScrapbookJson,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
            case Outcome.Postpone:
                return await _functionStore.PostponeFunction(
                    functionId,
                    postponeUntil: result.Postpone!.DateTime.Ticks,
                    scrapbookJson: storedScrapbook.ScrapbookJson,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
            case Outcome.Fail:
                return await _functionStore.FailFunction(
                    functionId,
                    storedException: Serializer.SerializeException(result.Fail!),
                    scrapbookJson: storedScrapbook.ScrapbookJson,
                    timestamp: DateTime.UtcNow.Ticks,
                    expectedEpoch,
                    complementaryState
                );
            case Outcome.Suspend:
                return await _functionStore.SuspendFunction(
                    functionId,
                    result.Suspend!.ExpectedMessageCount,
                    storedScrapbook.ScrapbookJson,
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

            var scrapbook = Serializer.DeserializeScrapbook<TScrapbook>(
                sf.Scrapbook.ScrapbookJson,
                sf.Scrapbook.ScrapbookType
            );
            scrapbook.Initialize(onSave: () => SaveScrapbook(functionId, param, scrapbook, sf.Epoch, _settings.LeaseLength.Ticks, sf.SendResultTo));
            
            return new PreparedReInvocation(param, sf.Epoch, scrapbook, runningFunction, sf.SendResultTo);
        }
        catch (DeserializationException e)
        {
            runningFunction.Dispose();
            var sf = await _functionStore.GetFunction(functionId);
            await _functionStore.FailFunction(
                functionId,
                storedException: Serializer.SerializeException(e),
                scrapbookJson: sf!.Scrapbook.ScrapbookJson,
                timestamp: DateTime.UtcNow.Ticks,
                expectedEpoch,
                complimentaryState: new ComplimentaryState(
    () => sf.Parameter, 
    () => sf.Scrapbook, 
                    _settings.LeaseLength.Ticks, 
                    sf.SendResultTo
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

    internal record PreparedReInvocation(TParam Param, int Epoch, TScrapbook Scrapbook, IDisposable RunningFunction, FunctionId? SendResultTo);

    public IDisposable StartLeaseUpdater(FunctionId functionId, int epoch = 0) 
        => LeaseUpdater.CreateAndStart(functionId, epoch, _functionStore, _settings);

    public async Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        TParam param,
        TScrapbook scrapbook,
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
            storedScrapbook: serializer.SerializeScrapbook(scrapbook),
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
        TScrapbook scrapbook,
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
            storedScrapbook: serializer.SerializeScrapbook(scrapbook),
            storedResult: result == null ? StoredResult.Null : serializer.SerializeResult(result),
            exception == null ? null : serializer.SerializeException(exception),
            postponeUntil?.Ticks,
            expectedEpoch
        );
    }

    public async Task<bool> SaveControlPanelChanges(
        FunctionId functionId, 
        TParam param, 
        TScrapbook scrapbook,
        TReturn? @return,
        int expectedEpoch)
    {
        var serializer = _settings.Serializer;
        return await _functionStore.SetParameters(
            functionId,
            storedParameter: serializer.SerializeParameter(param),
            storedScrapbook: serializer.SerializeScrapbook(scrapbook),
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
        

    public async Task<FunctionState<TParam, TScrapbook, TReturn>?> GetFunction(FunctionId functionId)
    {
        var serializer = _settings.Serializer;
        
        var sf = await _functionStore.GetFunction(functionId);
        if (sf == null) 
            return null;

        return new FunctionState<TParam, TScrapbook, TReturn>(
            functionId,
            sf.Status,
            sf.Epoch,
            sf.LeaseExpiration,
            Param: serializer.DeserializeParameter<TParam>(sf.Parameter.ParamJson, sf.Parameter.ParamType),
            Scrapbook: serializer.DeserializeScrapbook<TScrapbook>(sf.Scrapbook.ScrapbookJson, sf.Scrapbook.ScrapbookType),
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

    public async Task<List<MessageAndIdempotencyKey>> GetEvents(FunctionId functionId)
    {
        var storedMessages = await _functionStore.MessageStore.GetMessages(functionId);
        return storedMessages
            .Select(se => new MessageAndIdempotencyKey(
                    _settings.Serializer.DeserializeMessage(se.MessageJson, se.MessageType),
                    se.IdempotencyKey
                )
            )
            .ToList();
    }
    
    public async Task<ExistingMessages> GetExistingMessages(FunctionId functionId) 
        => new ExistingMessages(functionId, await GetEvents(functionId), _functionStore.MessageStore, _settings.Serializer);

    public ITimeoutProvider CreateTimeoutProvider(FunctionId functionId)
        => new TimeoutProvider(
            functionId,
            _functionStore.TimeoutStore,
            messageWriter: null,
            timeoutCheckFrequency: TimeSpan.Zero
        );
}