using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public class CrashableFunctionStore : IFunctionStore
{
    private readonly IFunctionStore _inner;
    private volatile bool _crashed;

    private readonly object _sync = new();
    private readonly Subject<long> _afterPostponeFunctionSubject = new();
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

    public Task<bool> CreateFunction(FunctionId functionId, StoredParameter param, StoredScrapbook storedScrapbook,
        long crashedCheckFrequency, int version)
        => _crashed 
            ? Task.FromException<bool>(new TimeoutException()) 
            : _inner.CreateFunction(
                functionId, 
                param, 
                storedScrapbook,
                crashedCheckFrequency,
                version
            );

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Tuple<StoredParameter, StoredScrapbook>? paramAndScrapbook, int expectedEpoch, int newEpoch, long crashedCheckFrequency, int version)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.TryToBecomeLeader(functionId, paramAndScrapbook, expectedEpoch, newEpoch, crashedCheckFrequency, version);
    
    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.UpdateSignOfLife(functionId, expectedEpoch, newSignOfLife);

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId, int versionUpperBound)
        => _crashed
            ? Task.FromException<IEnumerable<StoredExecutingFunction>>(new TimeoutException())
            : _inner.GetExecutingFunctions(functionTypeId, versionUpperBound);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore, int versionUpperBound)
        => _crashed
            ? Task.FromException<IEnumerable<StoredPostponedFunction>>(new TimeoutException())
            : _inner.GetPostponedFunctions(functionTypeId, expiresBefore, versionUpperBound);

    public Task<bool> SetFunctionState(FunctionId functionId, Status status, StoredParameter storedParameter,
        StoredScrapbook storedScrapbook, StoredResult storedResult, 
        string? errorJson, long? postponeUntil, int expectedEpoch) 
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetFunctionState(
                functionId, status, 
                storedParameter, storedScrapbook, storedResult, 
                errorJson, postponeUntil, expectedEpoch
            );
    
    public Task<bool> SetScrapbook(FunctionId functionId, string scrapbookJson, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetScrapbook(functionId, scrapbookJson, expectedEpoch);

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter? storedParameter, StoredScrapbook? storedScrapbook, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetParameters(functionId, storedParameter, storedScrapbook, expectedEpoch);

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

    public Task<bool> FailFunction(FunctionId functionId, string errorJson, string scrapbookJson, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.FailFunction(functionId, errorJson, scrapbookJson, expectedEpoch);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _crashed
            ? Task.FromException<StoredFunction?>(new TimeoutException())
            : _inner.GetFunction(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null, Status? expectedStatus = null)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.DeleteFunction(functionId, expectedEpoch, expectedStatus);
}

public static class CrashableFunctionStoreExtensions
{
    public static CrashableFunctionStore ToCrashableFunctionStore(this IFunctionStore store) => new(store);
}