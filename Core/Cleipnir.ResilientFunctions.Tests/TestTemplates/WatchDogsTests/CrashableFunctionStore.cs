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

    public Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        string? scrapbookType,
        long crashedCheckFrequency
    ) => _crashed 
        ? Task.FromException<bool>(new TimeoutException()) 
        : _inner.CreateFunction(
            functionId, 
            param, 
            scrapbookType,
            crashedCheckFrequency
        );

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.TryToBecomeLeader(functionId, newStatus, expectedEpoch, newEpoch, crashedCheckFrequency);

    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.UpdateSignOfLife(functionId, expectedEpoch, newSignOfLife);

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
        => _crashed
            ? Task.FromException<IEnumerable<StoredExecutingFunction>>(new TimeoutException())
            : _inner.GetExecutingFunctions(functionTypeId);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
        => _crashed
            ? Task.FromException<IEnumerable<StoredPostponedFunction>>(new TimeoutException())
            : _inner.GetPostponedFunctions(functionTypeId, expiresBefore);

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
        string? scrapbookJson,
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

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _crashed
            ? Task.FromException<StoredFunction?>(new TimeoutException())
            : _inner.GetFunction(functionId);
}

public static class CrashableFunctionStoreExtensions
{
    public static CrashableFunctionStore ToCrashableFunctionStore(this IFunctionStore store) => new(store);
}