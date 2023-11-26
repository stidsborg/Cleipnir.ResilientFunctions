using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public class CrashableFunctionStore : IFunctionStore
{
    private readonly IFunctionStore _inner;
    private volatile bool _crashed;

    private readonly object _sync = new();
    private readonly Subject<long> _afterPostponeFunctionSubject = new();
    public IEventStore EventStore => _inner.EventStore;
    public IActivityStore ActivityStore => _inner.ActivityStore;
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public Utilities Utilities => _inner.Utilities;

    public IObservable<long> AfterPostponeFunctionStream
    {
        get
        {
            lock (_sync)
                return _afterPostponeFunctionSubject;
        }
    }

    public CrashableFunctionStore(IFunctionStore inner) => _inner = inner;

    public void Crash() => _crashed = true;

    public Task Initialize() => Task.CompletedTask;

    public Task<bool> CreateFunction(
        FunctionId functionId,
        StoredParameter param,
        StoredScrapbook storedScrapbook,
        IEnumerable<StoredEvent>? storedEvents,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.CreateFunction(
            functionId,
            param,
            storedScrapbook,
            storedEvents,
            leaseExpiration,
            postponeUntil,
            timestamp
        );

    public Task<bool> IncrementAlreadyPostponedFunctionEpoch(FunctionId functionId, int expectedEpoch)
        => _inner.IncrementAlreadyPostponedFunctionEpoch(functionId, expectedEpoch);

    public Task<bool> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.RestartExecution(functionId, expectedEpoch, leaseExpiration);
    
    public Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.RenewLease(functionId, expectedEpoch, leaseExpiration);

    public Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
        => _crashed
            ? Task.FromException<IEnumerable<StoredExecutingFunction>>(new TimeoutException())
            : _inner.GetCrashedFunctions(functionTypeId, leaseExpiresBefore);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
        => _crashed
            ? Task.FromException<IEnumerable<StoredPostponedFunction>>(new TimeoutException())
            : _inner.GetPostponedFunctions(functionTypeId, expiresBefore);

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status, StoredParameter storedParameter,
        StoredScrapbook storedScrapbook, StoredResult storedResult, StoredException? storedException, 
        long? postponeUntil, 
        int expectedEpoch
    ) => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetFunctionState(
                functionId, status, 
                storedParameter, storedScrapbook, storedResult, 
                storedException, postponeUntil, 
                expectedEpoch
            );

    public Task<bool> SaveScrapbookForExecutingFunction( 
        FunctionId functionId,
        string scrapbookJson,
        int expectedEpoch,
        ComplimentaryState.SaveScrapbookForExecutingFunction complimentaryState) 
    => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.SaveScrapbookForExecutingFunction(functionId, scrapbookJson, expectedEpoch, complimentaryState);

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetParameters(functionId, storedParameter, storedScrapbook, storedResult, expectedEpoch);

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, long timestamp, int expectedEpoch, ComplimentaryState.SetResult complementaryState)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SucceedFunction(functionId, result, scrapbookJson, timestamp, expectedEpoch, complementaryState);

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, long timestamp, int expectedEpoch, ComplimentaryState.SetResult complementaryState)
    {
        if (_crashed)
            return Task.FromException<bool>(new TimeoutException());
                
        var success = _inner.PostponeFunction(functionId, postponeUntil, scrapbookJson, timestamp, expectedEpoch, complementaryState);
        _afterPostponeFunctionSubject.OnNext(postponeUntil);
        return success;
    }

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, long timestamp, int expectedEpoch, ComplimentaryState.SetResult complementaryState)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.FailFunction(functionId, storedException, scrapbookJson, timestamp, expectedEpoch, complementaryState);

    public Task<SuspensionResult> SuspendFunction(FunctionId functionId, int expectedEventCount, string scrapbookJson, long timestamp, int expectedEpoch, ComplimentaryState.SetResult complementaryState)
        => _crashed
            ? Task.FromException<SuspensionResult>(new TimeoutException())
            : _inner.SuspendFunction(functionId, expectedEventCount, scrapbookJson, timestamp, expectedEpoch, complementaryState);

    public Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
        => _crashed
            ? Task.FromException<StatusAndEpoch?>(new TimeoutException())
            : _inner.GetFunctionStatus(functionId);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _crashed
            ? Task.FromException<StoredFunction?>(new TimeoutException())
            : _inner.GetFunction(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.DeleteFunction(functionId, expectedEpoch);
}

public static class CrashableFunctionStoreExtensions
{
    public static CrashableFunctionStore ToCrashableFunctionStore(this IFunctionStore store) => new(store);
}