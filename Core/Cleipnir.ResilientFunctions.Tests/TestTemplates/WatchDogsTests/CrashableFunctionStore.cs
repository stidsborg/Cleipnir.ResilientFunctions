using System;
using System.Collections.Generic;
using System.Reactive.Subjects;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public class CrashableFunctionStore : IFunctionStore
{
    private readonly IFunctionStore _inner;
    private volatile bool _crashed;

    private readonly object _sync = new();
    private readonly Subject<SetFunctionStateParams> _subject = new();

    public IObservable<SetFunctionStateParams> AfterSetFunctionStateStream
    {
        get
        {
            lock (_sync)
                return _subject;
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

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency, int version)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.TryToBecomeLeader(functionId, newStatus, expectedEpoch, newEpoch, crashedCheckFrequency, version);

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency, int version, string scrapbookJson)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.TryToBecomeLeader(functionId, newStatus, expectedEpoch, newEpoch, crashedCheckFrequency, version, scrapbookJson);

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

    public record SetFunctionStateParams(
        FunctionId FunctionId,
        Status Status,
        string? ScrapbookJson,
        StoredResult? Result,
        string? ErrorJson,
        long? PostponedUntil,
        int ExpectedEpoch
    );
    
    public async Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string scrapbookJson,
        StoredResult? result,
        string? errorJson,
        long? postponedUntil,
        int expectedEpoch
    )
    {
        if (_crashed)
            throw new TimeoutException();

        var success = await _inner
            .SetFunctionState(
                functionId,
                status,
                scrapbookJson,
                result,
                errorJson,
                postponedUntil,
                expectedEpoch
            );

        Subject<SetFunctionStateParams> subject;
        lock (_sync)
            subject = _subject;
        
        subject.OnNext(new SetFunctionStateParams(
            functionId, status, scrapbookJson, result, errorJson, postponedUntil, expectedEpoch
        ));

        return success;
    }

    public Task<bool> SetScrapbook(FunctionId functionId, string scrapbookJson, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetScrapbook(functionId, scrapbookJson, expectedEpoch);

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