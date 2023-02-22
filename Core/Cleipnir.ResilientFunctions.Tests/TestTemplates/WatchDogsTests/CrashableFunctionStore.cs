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

    public Task<bool> CreateFunction(FunctionId functionId, StoredParameter param, StoredScrapbook storedScrapbook, long crashedCheckFrequency)
        => _crashed 
            ? Task.FromException<bool>(new TimeoutException()) 
            : _inner.CreateFunction(
                functionId, 
                param, 
                storedScrapbook,
                crashedCheckFrequency
            );

    public Task<bool> IncrementAlreadyPostponedFunctionEpoch(FunctionId functionId, int expectedEpoch)
        => _inner.IncrementAlreadyPostponedFunctionEpoch(functionId, expectedEpoch);

    public Task<bool> RestartExecution(FunctionId functionId, int expectedEpoch, long crashedCheckFrequency)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.RestartExecution(functionId, expectedEpoch, crashedCheckFrequency);
    
    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife, ComplimentaryState.UpdateSignOfLife complementaryState)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.UpdateSignOfLife(functionId, expectedEpoch, newSignOfLife, complementaryState);

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
        => _crashed
            ? Task.FromException<IEnumerable<StoredExecutingFunction>>(new TimeoutException())
            : _inner.GetExecutingFunctions(functionTypeId);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
        => _crashed
            ? Task.FromException<IEnumerable<StoredPostponedFunction>>(new TimeoutException())
            : _inner.GetPostponedFunctions(functionTypeId, expiresBefore);

    public Task<IEnumerable<StoredEligibleSuspendedFunction>> GetEligibleSuspendedFunctions(FunctionTypeId functionTypeId)
        => _crashed
            ? Task.FromException<IEnumerable<StoredEligibleSuspendedFunction>>(new TimeoutException())
            : _inner.GetEligibleSuspendedFunctions(functionTypeId);

    public Task<Epoch?> IsFunctionSuspendedAndEligibleForReInvocation(FunctionId functionId)
        => _crashed
            ? Task.FromException<Epoch?>(new TimeoutException())
            : _inner.IsFunctionSuspendedAndEligibleForReInvocation(functionId);

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status, StoredParameter storedParameter,
        StoredScrapbook storedScrapbook, StoredResult storedResult, StoredException? storedException, 
        long? postponeUntil, 
        ReplaceEvents? events,
        int expectedEpoch
    ) => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetFunctionState(
                functionId, status, 
                storedParameter, storedScrapbook, storedResult, 
                storedException, postponeUntil, 
                events,
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

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredScrapbook storedScrapbook, ReplaceEvents? events, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetParameters(functionId, storedParameter, storedScrapbook, events, expectedEpoch);

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SucceedFunction(functionId, result, scrapbookJson, expectedEpoch);

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch)
    {
        if (_crashed)
            return Task.FromException<bool>(new TimeoutException());
                
        var success = _inner.PostponeFunction(functionId, postponeUntil, scrapbookJson, expectedEpoch);
        _afterPostponeFunctionSubject.OnNext(postponeUntil);
        return success;
    }

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.FailFunction(functionId, storedException, scrapbookJson, expectedEpoch);

    public Task<bool> SuspendFunction(FunctionId functionId, int suspendUntilEventSourceCountAtLeast, string scrapbookJson, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SuspendFunction(functionId, suspendUntilEventSourceCountAtLeast, scrapbookJson, expectedEpoch);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _crashed
            ? Task.FromException<StoredFunction?>(new TimeoutException())
            : _inner.GetFunction(functionId);

    public Task<StoredFunctionStatus?> GetFunctionStatus(FunctionId functionId)
        => _crashed
            ? Task.FromException<StoredFunctionStatus?>(new TimeoutException())
            : _inner.GetFunctionStatus(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.DeleteFunction(functionId, expectedEpoch);
}

public static class CrashableFunctionStoreExtensions
{
    public static CrashableFunctionStore ToCrashableFunctionStore(this IFunctionStore store) => new(store);
}