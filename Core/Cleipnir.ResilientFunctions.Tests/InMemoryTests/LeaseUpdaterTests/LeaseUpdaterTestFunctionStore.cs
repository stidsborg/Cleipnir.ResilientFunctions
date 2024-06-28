using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.LeaseUpdaterTests;

public class LeaseUpdaterTestFunctionStore : IFunctionStore
{
    public delegate bool LeaseUpdaterCallback(FunctionId functionId, int expectedEpoch, long newLeaseExpiry);

    private readonly LeaseUpdaterCallback _leaseUpdaterCallback;
    private readonly IFunctionStore _inner = new InMemoryFunctionStore();

    public LeaseUpdaterTestFunctionStore(LeaseUpdaterCallback leaseUpdaterCallback) => _leaseUpdaterCallback = leaseUpdaterCallback;

    public IMessageStore MessageStore => _inner.MessageStore;
    public IEffectsStore EffectsStore => _inner.EffectsStore;
    public IStatesStore StatesStore => _inner.StatesStore;
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public ICorrelationStore CorrelationStore => _inner.CorrelationStore;
    public Utilities Utilities => _inner.Utilities;
    public Task Initialize() => _inner.Initialize();

    public Task<bool> CreateFunction(
        FunctionId functionId, 
        string? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    ) => _inner.CreateFunction(functionId, param, leaseExpiration, postponeUntil, timestamp);

    public Task BulkScheduleFunctions(IEnumerable<FunctionIdWithParam> functionsWithParam)
        => _inner.BulkScheduleFunctions(functionsWithParam);

    public Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
        => _inner.RestartExecution(functionId, expectedEpoch, leaseExpiration);
    
    public Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        var success = _leaseUpdaterCallback(functionId, expectedEpoch, leaseExpiration);
        return success.ToTask();
    }

    public Task<IReadOnlyList<InstanceIdAndEpoch>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
        => _inner.GetCrashedFunctions(functionTypeId, leaseExpiresBefore);

    public Task<IReadOnlyList<InstanceIdAndEpoch>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
        => _inner.GetPostponedFunctions(functionTypeId, isEligibleBefore);

    public Task<IReadOnlyList<FunctionInstanceId>> GetSucceededFunctions(FunctionTypeId functionTypeId, long completedBefore)
        => _inner.GetSucceededFunctions(functionTypeId, completedBefore);

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status, 
        string? storedParameter, string? storedResult, 
        StoredException? storedException, 
        long? postponeUntil, 
        int expectedEpoch
    ) => _inner.SetFunctionState(functionId, status, storedParameter, storedResult, storedException, postponeUntil, expectedEpoch);

    public Task<bool> SucceedFunction(
        FunctionId functionId, 
        string? result, 
        string? defaultState, 
        long timestamp, 
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _inner.SucceedFunction(functionId, result, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> PostponeFunction(
        FunctionId functionId,
        long postponeUntil,
        string? defaultState,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _inner.PostponeFunction(functionId, postponeUntil, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> FailFunction(
        FunctionId functionId, 
        StoredException storedException, 
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _inner.FailFunction(functionId, storedException, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> SuspendFunction(
        FunctionId functionId,
        long expectedInterruptCount,
        string? defaultState,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _inner.SuspendFunction(functionId, expectedInterruptCount, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task SetDefaultState(FunctionId functionId, string? stateJson)
        => _inner.SetDefaultState(functionId, stateJson); 

    public Task<bool> SetParameters(FunctionId functionId, string? storedParameter, string? storedResult, int expectedEpoch)
        => _inner.SetParameters(functionId, storedParameter, storedResult, expectedEpoch);
    
    public Task<bool> IncrementInterruptCount(FunctionId functionId)
        => _inner.IncrementInterruptCount(functionId);

    public Task<long?> GetInterruptCount(FunctionId functionId)
        => _inner.GetInterruptCount(functionId);

    public Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
        => _inner.GetFunctionStatus(functionId);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _inner.GetFunction(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId) => _inner.DeleteFunction(functionId);
}