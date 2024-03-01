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
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public Utilities Utilities => _inner.Utilities;
    public Task Initialize() => _inner.Initialize();

    public Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        StoredState storedState, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    ) => _inner.CreateFunction(functionId, param, storedState, leaseExpiration, postponeUntil, timestamp);
    
    public Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
        => _inner.RestartExecution(functionId, expectedEpoch, leaseExpiration);
    
    public Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        var success = _leaseUpdaterCallback(functionId, expectedEpoch, leaseExpiration);
        return success.ToTask();
    }

    public Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
        => _inner.GetCrashedFunctions(functionTypeId, leaseExpiresBefore);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
        => _inner.GetPostponedFunctions(functionTypeId, isEligibleBefore);

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status, 
        StoredParameter storedParameter, StoredState storedState, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil, 
        int expectedEpoch
    ) => _inner.SetFunctionState(functionId, status, storedParameter, storedState, storedResult, storedException, postponeUntil, expectedEpoch);

    public Task<bool> SaveStateForExecutingFunction( 
        FunctionId functionId,
        string stateJson,
        int expectedEpoch,
        ComplimentaryState complimentaryState) 
    => _inner.SaveStateForExecutingFunction(functionId, stateJson, expectedEpoch, complimentaryState);

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredState storedState, StoredResult storedResult, int expectedEpoch)
        => _inner.SetParameters(functionId, storedParameter, storedState, storedResult, expectedEpoch);

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _inner.SucceedFunction(functionId, result, stateJson, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _inner.PostponeFunction(functionId, postponeUntil, stateJson, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _inner.FailFunction(functionId, storedException, stateJson, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> SuspendFunction(FunctionId functionId, int expectedMessageCount, string stateJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _inner.SuspendFunction(functionId, expectedMessageCount, stateJson, timestamp, expectedEpoch, complimentaryState);

    public Task IncrementSignalCount(FunctionId functionId)
        => _inner.IncrementSignalCount(functionId);

    public Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
        => _inner.GetFunctionStatus(functionId);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _inner.GetFunction(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
        => _inner.DeleteFunction(functionId, expectedEpoch);
}