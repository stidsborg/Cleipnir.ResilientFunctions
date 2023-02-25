using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using EventAndIdempotencyKey = Cleipnir.ResilientFunctions.Messaging.EventAndIdempotencyKey;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal class InvocationHelper<TParam, TScrapbook, TReturn> 
    where TParam : notnull where TScrapbook : RScrapbook, new() 
{
    private readonly ShutdownCoordinator _shutdownCoordinator;
    private readonly IFunctionStore _functionStore;
    private readonly SettingsWithDefaults _settings;

    private ISerializer Serializer { get; }

    public InvocationHelper(
        SettingsWithDefaults settings,
        IFunctionStore functionStore,
        ShutdownCoordinator shutdownCoordinator)
    {
        _settings = settings;

        Serializer = new ErrorHandlingDecorator(settings.Serializer);
        _shutdownCoordinator = shutdownCoordinator;
        _functionStore = functionStore;
    }

    public async Task<Tuple<bool, IDisposable>> PersistFunctionInStore(FunctionId functionId, TParam param, TScrapbook scrapbook)
    {
        ArgumentNullException.ThrowIfNull(param);
        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        try
        {
            var storedParameter = Serializer.SerializeParameter(param);
            var storedScrapbook = Serializer.SerializeScrapbook(scrapbook);
            var created = await _functionStore.CreateFunction(
                functionId,
                storedParameter,
                storedScrapbook,
                crashedCheckFrequency: _settings.CrashedCheckFrequency.Ticks
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
    
    public async Task<TReturn> WaitForFunctionResult(FunctionId functionId) 
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
                    throw new FunctionInvocationPostponedException(
                        functionId,
                        postponedUntil: new DateTime(storedFunction.PostponedUntil!.Value, DateTimeKind.Utc)
                    );
                default:
                    throw new ArgumentOutOfRangeException(); 
            }
        }
    }

    public void InitializeScrapbook(FunctionId functionId, TParam param, TScrapbook scrapbook, int epoch) 
        => scrapbook.Initialize(onSave: () => SaveScrapbook(functionId, param, scrapbook, epoch));

    private async Task SaveScrapbook(FunctionId functionId, TParam param, TScrapbook scrapbook, int epoch)
    {
        var storedParameter = Serializer.SerializeParameter(param);
        var storedScrapbook = Serializer.SerializeScrapbook(scrapbook);
        
        var success = await _functionStore.SaveScrapbookForExecutingFunction(
            functionId,
            storedScrapbook.ScrapbookJson,
            expectedEpoch: epoch,
            complimentaryState: new ComplimentaryState.SaveScrapbookForExecutingFunction(storedParameter, storedScrapbook)
        );

        if (!success)
            throw new ScrapbookSaveFailedException(
                functionId,
                $"Unable to save '{functionId}'-scrapbook due to concurrent modification"
            );
    }
    
    public async Task PersistFailure(FunctionId functionId, Exception exception, TScrapbook scrapbook, int expectedEpoch)
    {
        var success = await _functionStore.FailFunction(
            functionId,
            storedException: Serializer.SerializeException(exception),
            scrapbookJson: Serializer.SerializeScrapbook(scrapbook).ScrapbookJson,
            expectedEpoch
        );
        if (!success) 
            throw new ConcurrentModificationException(functionId);
    }

    public async Task PersistResult(
        FunctionId functionId,
        Result<TReturn> result,
        TParam param,
        TScrapbook scrapbook,
        int expectedEpoch)
    {
        var complementaryState = new ComplimentaryState.SetResult(
            Serializer.SerializeParameter(param),
            Serializer.SerializeScrapbook(scrapbook)
        );
        switch (result.Outcome)
        {
            case Outcome.Succeed:
                var success = await _functionStore.SucceedFunction(
                    functionId,
                    result: Serializer.SerializeResult(result.SucceedWithValue),
                    scrapbookJson: complementaryState.StoredScrapbook.ScrapbookJson,
                    expectedEpoch,
                    complementaryState
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
            case Outcome.Postpone:
                success = await _functionStore.PostponeFunction(
                    functionId,
                    postponeUntil: result.Postpone!.DateTime.Ticks,
                    scrapbookJson: Serializer.SerializeScrapbook(scrapbook).ScrapbookJson,
                    expectedEpoch
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
            case Outcome.Fail:
                success = await _functionStore.FailFunction(
                    functionId,
                    storedException: Serializer.SerializeException(result.Fail!),
                    scrapbookJson: Serializer.SerializeScrapbook(scrapbook).ScrapbookJson,
                    expectedEpoch
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
            case Outcome.Suspend:
                success = await _functionStore.SuspendFunction(
                    functionId,
                    result.Suspend!.UntilEventSourceCount,
                    Serializer.SerializeScrapbook(scrapbook).ScrapbookJson,
                    expectedEpoch
                );
                if (!success) throw new ConcurrentModificationException(functionId);
                return;
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

    public async Task<PreparedReInvocation> PrepareForReInvocation(
        FunctionId functionId, int expectedEpoch
    ) 
    {
        var sf = await _functionStore.GetFunction(functionId);
        if (sf == null)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' not found");
        if (sf.Epoch != expectedEpoch)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' did not have expected epoch: '{sf.Epoch}'");

        var runningFunction = _shutdownCoordinator.RegisterRunningRFunc();
        var epoch = sf.Epoch + 1;
        try
        {
            var success = await _functionStore.RestartExecution(
                functionId,
                expectedEpoch: sf.Epoch,
                _settings.CrashedCheckFrequency.Ticks
            );

            if (!success)
                throw new UnexpectedFunctionState(functionId, $"Unable to become leader for function: '{functionId}'");

            var param = Serializer.DeserializeParameter<TParam>(sf.Parameter.ParamJson, sf.Parameter.ParamType);

            var scrapbook = Serializer.DeserializeScrapbook<TScrapbook>(
                sf.Scrapbook.ScrapbookJson,
                sf.Scrapbook.ScrapbookType
            );
            scrapbook.Initialize(onSave: () => SaveScrapbook(functionId, param, scrapbook, epoch));
            
            return new PreparedReInvocation(param, epoch, scrapbook, runningFunction);
        }
        catch (DeserializationException e)
        {
            runningFunction.Dispose();
            await _functionStore.FailFunction(
                functionId,
                storedException: Serializer.SerializeException(e),
                scrapbookJson: sf.Scrapbook.ScrapbookJson,
                expectedEpoch: epoch
            );
            throw;
        }
        catch (Exception)
        {
            runningFunction.Dispose();
            throw;
        }
    }

    internal record PreparedReInvocation(TParam Param, int Epoch, TScrapbook Scrapbook, IDisposable RunningFunction);

    public IDisposable StartSignOfLife(FunctionId functionId, int epoch = 0) 
        => SignOfLifeUpdater.CreateAndStart(functionId, epoch, _functionStore, _settings);

    public async Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        TParam param,
        TScrapbook scrapbook,
        DateTime? postponeUntil,
        Exception? exception,
        ExistingEvents? existingEvents,
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
            existingEvents == null 
                ? null 
                : new ReplaceEvents(
                    Events: existingEvents
                        .EventsWithIdempotencyKeys
                        .Select(e =>
                        {
                            var (json, type) = _settings.Serializer.SerializeEvent(e.Event);
                            return new StoredEvent(json, type, e.IdempotencyKey);
                        })
                        .ToList(), 
                    existingEvents.ExistingCount
                  ),
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
        ExistingEvents? existingEvents,
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
            existingEvents == null 
                ? null 
                : new ReplaceEvents(
                    Events: existingEvents
                        .EventsWithIdempotencyKeys
                        .Select(e =>
                        {
                            var (json, type) = _settings.Serializer.SerializeEvent(e.Event);
                            return new StoredEvent(json, type, e.IdempotencyKey);
                        })
                        .ToList(), 
                    existingEvents.ExistingCount
                ),
            expectedEpoch
        );
    }

    public async Task<bool> SaveControlPanelChanges(
        FunctionId functionId, 
        TParam param, 
        TScrapbook scrapbook, 
        ExistingEvents? existingEvents, 
        int expectedEpoch)
    {
        var serializer = _settings.Serializer;
        return await _functionStore.SetParameters(
            functionId,
            storedParameter: serializer.SerializeParameter(param),
            storedScrapbook: serializer.SerializeScrapbook(scrapbook),
            existingEvents == null 
                ? null 
                : new ReplaceEvents(
                    Events: existingEvents
                        .EventsWithIdempotencyKeys
                        .Select(e =>
                        {
                            var (json, type) = _settings.Serializer.SerializeEvent(e.Event);
                            return new StoredEvent(json, type, e.IdempotencyKey);
                        })
                        .ToList(), 
                    existingEvents.ExistingCount
                ),
            expectedEpoch
        );
    }

    public async Task<bool> Delete(FunctionId functionId, int expectedEpoch)
        => await _functionStore.DeleteFunction(functionId, expectedEpoch);

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
            sf.CrashedCheckFrequency, 
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

    public Func<Task<EventSource>> CreateAndInitializeEventSource(FunctionId functionId, ScheduleReInvocation scheduleReInvocation)
    {
        async Task<EventSource> CreateNewEventSource()
        {
            var eventSourceWriter = new EventSourceWriter(functionId, _functionStore, Serializer, scheduleReInvocation);
            var timeoutProvider = new TimeoutProvider(functionId, _functionStore.TimeoutStore, eventSourceWriter, _settings.TimeoutCheckFrequency); 
            var eventSource = new EventSource(
                functionId,
                _functionStore.EventStore,
                eventSourceWriter,
                timeoutProvider,
                _settings.EventSourcePullFrequency,
                _settings.Serializer
            );
            await eventSource.Initialize();

            return eventSource;
        }

        return CreateNewEventSource;
    }

    public async Task<List<EventAndIdempotencyKey>> GetEvents(FunctionId functionId)
    {
        var storedEvents = await _functionStore.EventStore.GetEvents(functionId);
        return storedEvents
            .Select(se => new EventAndIdempotencyKey(
                    _settings.Serializer.DeserializeEvent(se.EventJson, se.EventType),
                    se.IdempotencyKey
                )
            )
            .ToList();
    }
    
    public async Task<ExistingEvents> GetExistingEvents(FunctionId functionId) 
        => new ExistingEvents(await GetEvents(functionId));
}