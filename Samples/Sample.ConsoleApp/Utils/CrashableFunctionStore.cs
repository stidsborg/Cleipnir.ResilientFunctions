using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.Utils;

public class CrashableFunctionStore : IFunctionStore
{
    private readonly IFunctionStore _inner;
    private volatile bool _crashed;

    private readonly object _sync = new();
    public IMessageStore MessageStore => _inner.MessageStore;
    public IActivityStore ActivityStore => _inner.ActivityStore;
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public Utilities Utilities => _inner.Utilities;
    
    public CrashableFunctionStore(IFunctionStore inner) => _inner = inner;

    public void Crash() => _crashed = true;

    public Task Initialize() => Task.CompletedTask;

    public Task<bool> CreateFunction(
        FunctionId functionId,
        StoredParameter param,
        StoredState storedState,
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.CreateFunction(
            functionId,
            param,
            storedState,
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

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status, StoredParameter storedParameter,
        StoredState storedState, StoredResult storedResult, StoredException? storedException, 
        long? postponeUntil, 
        int expectedEpoch
    ) => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetFunctionState(
                functionId, status, 
                storedParameter, storedState, storedResult, 
                storedException, postponeUntil, 
                expectedEpoch
            );

    public Task<bool> SaveStateForExecutingFunction( 
        FunctionId functionId,
        string stateJson,
        int expectedEpoch,
        ComplimentaryState complimentaryState) 
    => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.SaveStateForExecutingFunction(functionId, stateJson, expectedEpoch, complimentaryState);

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredState storedState, StoredResult storedResult, int expectedEpoch)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetParameters(functionId, storedParameter, storedState, storedResult, expectedEpoch);

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SucceedFunction(functionId, result, stateJson, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        if (_crashed)
            return Task.FromException<bool>(new TimeoutException());
                
        return _inner.PostponeFunction(functionId, postponeUntil, stateJson, timestamp, expectedEpoch, complimentaryState);
    }

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.FailFunction(functionId, storedException, stateJson, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> SuspendFunction(FunctionId functionId, int expectedMessageCount, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SuspendFunction(functionId, expectedMessageCount, stateJson, timestamp, expectedEpoch, complimentaryState);

    public Task IncrementSignalCount(FunctionId functionId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.IncrementSignalCount(functionId);

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