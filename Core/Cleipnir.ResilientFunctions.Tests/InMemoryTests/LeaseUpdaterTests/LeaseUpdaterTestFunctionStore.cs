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
    public Utilities Utilities => _inner.Utilities;
    public Task Initialize() => _inner.Initialize();

    public Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    ) => _inner.CreateFunction(functionId, param, leaseExpiration, postponeUntil, timestamp);
    
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
        StoredParameter storedParameter, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil, 
        int expectedEpoch
    ) => _inner.SetFunctionState(functionId, status, storedParameter, storedResult, storedException, postponeUntil, expectedEpoch);

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredResult storedResult, int expectedEpoch)
        => _inner.SetParameters(functionId, storedParameter, storedResult, expectedEpoch);

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _inner.SucceedFunction(functionId, result, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _inner.PostponeFunction(functionId, postponeUntil, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _inner.FailFunction(functionId, storedException, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> SuspendFunction(FunctionId functionId, long expectedInterruptCount, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
        => _inner.SuspendFunction(functionId, expectedInterruptCount, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> IncrementInterruptCount(FunctionId functionId)
        => _inner.IncrementInterruptCount(functionId);

    public Task<long?> GetInterruptCount(FunctionId functionId)
        => _inner.GetInterruptCount(functionId);

    public Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
        => _inner.GetFunctionStatus(functionId);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _inner.GetFunction(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
        => _inner.DeleteFunction(functionId, expectedEpoch);
}