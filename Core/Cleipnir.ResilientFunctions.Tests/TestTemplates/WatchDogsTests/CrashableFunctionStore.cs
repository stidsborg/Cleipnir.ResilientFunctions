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
    public IMessageStore MessageStore => _inner.MessageStore;
    public IEffectsStore EffectsStore => _inner.EffectsStore;
    public IStatesStore StatesStore => _inner.StatesStore;
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public ICorrelationStore CorrelationStore => _inner.CorrelationStore;
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
        string? param,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.CreateFunction(
            functionId,
            param,
            leaseExpiration,
            postponeUntil,
            timestamp
        );
    
    public Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
        => _crashed
            ? Task.FromException<StoredFunction?>(new TimeoutException())
            : _inner.RestartExecution(functionId, expectedEpoch, leaseExpiration);
    
    public Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.RenewLease(functionId, expectedEpoch, leaseExpiration);

    public Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
        => _crashed
            ? Task.FromException<IEnumerable<StoredExecutingFunction>>(new TimeoutException())
            : _inner.GetCrashedFunctions(functionTypeId, leaseExpiresBefore);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
        => _crashed
            ? Task.FromException<IEnumerable<StoredPostponedFunction>>(new TimeoutException())
            : _inner.GetPostponedFunctions(functionTypeId, isEligibleBefore);

    public Task<IReadOnlyList<FunctionInstanceId>> GetSucceededFunctions(FunctionTypeId functionTypeId, long completedBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<FunctionInstanceId>>(new TimeoutException())
            : _inner.GetSucceededFunctions(functionTypeId, completedBefore);

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status, string? storedParameter,
        string? storedResult, StoredException? storedException, 
        long? postponeUntil, 
        int expectedEpoch
    ) => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetFunctionState(
                functionId, status, 
                storedParameter, storedResult, 
                storedException, postponeUntil, 
                expectedEpoch
            );

    public Task<bool> SucceedFunction(
        FunctionId functionId,
        string? result,
        string? defaultState,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.SucceedFunction(functionId, result, defaultState, timestamp, expectedEpoch, complimentaryState);

    public async Task<bool> PostponeFunction(
        FunctionId functionId,
        long postponeUntil,
        string? defaultState,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    )
    {
        if (_crashed)
            throw new TimeoutException();

        var result = await _inner.PostponeFunction(functionId, postponeUntil, defaultState, timestamp, expectedEpoch, complimentaryState);
        _afterPostponeFunctionSubject.OnNext(postponeUntil);

        return result;
    } 

    public Task<bool> FailFunction(
        FunctionId functionId,
        StoredException storedException,
        string? defaultState,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.FailFunction(functionId, storedException, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> SuspendFunction(
        FunctionId functionId,
        long expectedInterruptCount,
        string? defaultState,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.SuspendFunction(functionId, expectedInterruptCount, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task SetDefaultState(FunctionId functionId, string? stateJson)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.SetDefaultState(functionId, stateJson);

    public Task<bool> SetParameters(FunctionId functionId, string? storedParameter, string? storedResult, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetParameters(functionId, storedParameter, storedResult, expectedEpoch);
    
    public Task<bool> IncrementInterruptCount(FunctionId functionId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.IncrementInterruptCount(functionId);

    public Task<long?> GetInterruptCount(FunctionId functionId)
        => _crashed
            ? Task.FromException<long?>(new TimeoutException())
            : _inner.GetInterruptCount(functionId);

    public Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
        => _crashed
            ? Task.FromException<StatusAndEpoch?>(new TimeoutException())
            : _inner.GetFunctionStatus(functionId);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _crashed
            ? Task.FromException<StoredFunction?>(new TimeoutException())
            : _inner.GetFunction(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.DeleteFunction(functionId);
}

public static class CrashableFunctionStoreExtensions
{
    public static CrashableFunctionStore ToCrashableFunctionStore(this IFunctionStore store) => new(store);
}