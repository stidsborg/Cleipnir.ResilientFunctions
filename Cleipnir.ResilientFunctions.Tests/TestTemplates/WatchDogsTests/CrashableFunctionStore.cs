using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public class CrashableFunctionStore : IFunctionStore
{
    private readonly IFunctionStore _inner;
    private volatile bool _crashed;

    private readonly object _sync = new();

    public Task AfterSetFunctionState
    {
        get
        {
            lock (_sync)
                return _afterSetFunctionState.Task;
        }
    }

    private TaskCompletionSource _afterSetFunctionState = new();

    public CrashableFunctionStore(IFunctionStore inner) => _inner = inner;

    public void Crash() => _crashed = true;
    
    public Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        string? scrapbookType, 
        Status initialStatus,
        int initialEpoch, 
        int initialSignOfLife
    ) => _crashed 
        ? Task.FromException<bool>(new TimeoutException()) 
        : _inner.CreateFunction(functionId, param, scrapbookType, initialStatus, initialEpoch, initialSignOfLife);

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.TryToBecomeLeader(functionId, newStatus, expectedEpoch, newEpoch);

    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.UpdateSignOfLife(functionId, expectedEpoch, newSignOfLife);

    public Task<IEnumerable<StoredFunctionStatus>> GetFunctionsWithStatus(
        FunctionTypeId functionTypeId,
        Status status,
        long? expiresBefore = null
    ) => _crashed
        ? Task.FromException<IEnumerable<StoredFunctionStatus>>(new TimeoutException())
        : _inner.GetFunctionsWithStatus(functionTypeId, status, expiresBefore);

    public Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string? scrapbookJson,
        StoredResult? result,
        StoredFailure? failed,
        long? postponedUntil,
        int expectedEpoch
    )
    {
        if (_crashed)
            return Task.FromException<bool>(new TimeoutException());
        
        return _inner.SetFunctionState(
                functionId,
                status,
                scrapbookJson,
                result,
                failed,
                postponedUntil,
                expectedEpoch
            ).ContinueWith(t =>
        {
            TaskCompletionSource afterSetFunctionState;
            lock (_sync)
            {
                afterSetFunctionState = _afterSetFunctionState;
                _afterSetFunctionState = new TaskCompletionSource();
            }
            Task.Run(afterSetFunctionState.SetResult);
            return t.Result;
        });  
    } 

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _crashed
            ? Task.FromException<StoredFunction?>(new TimeoutException())
            : _inner.GetFunction(functionId);
}

public static class CrashableFunctionStoreExtensions
{
    public static CrashableFunctionStore ToCrashableFunctionStore(this IFunctionStore store)
        => new CrashableFunctionStore(store);
}