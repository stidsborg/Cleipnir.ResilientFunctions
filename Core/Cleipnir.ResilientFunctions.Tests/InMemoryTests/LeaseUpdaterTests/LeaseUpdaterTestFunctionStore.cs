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
    public delegate bool LeaseUpdaterCallback(FlowId flowId, int expectedEpoch, long newLeaseExpiry);

    private readonly LeaseUpdaterCallback _leaseUpdaterCallback;
    private readonly IFunctionStore _inner = new InMemoryFunctionStore();

    public LeaseUpdaterTestFunctionStore(LeaseUpdaterCallback leaseUpdaterCallback) => _leaseUpdaterCallback = leaseUpdaterCallback;

    public IMessageStore MessageStore => _inner.MessageStore;
    public IEffectsStore EffectsStore => _inner.EffectsStore;
    public IStatesStore StatesStore => _inner.StatesStore;
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public ICorrelationStore CorrelationStore => _inner.CorrelationStore;
    public IReplicaStore ReplicaStore => _inner.ReplicaStore;
    public Utilities Utilities => _inner.Utilities;
    public IMigrator Migrator => _inner.Migrator;
    public Task Initialize() => _inner.Initialize();

    public Task<bool> CreateFunction(
        FlowId flowId, 
        string? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp
    ) => _inner.CreateFunction(flowId, param, leaseExpiration, postponeUntil, timestamp);

    public Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam)
        => _inner.BulkScheduleFunctions(functionsWithParam);

    public Task<StoredFlow?> RestartExecution(FlowId flowId, int expectedEpoch, long leaseExpiration)
        => _inner.RestartExecution(flowId, expectedEpoch, leaseExpiration);
    
    public Task<bool> RenewLease(FlowId flowId, int expectedEpoch, long leaseExpiration)
    {
        var success = _leaseUpdaterCallback(flowId, expectedEpoch, leaseExpiration);
        return success.ToTask();
    }

    public Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore)
        => _inner.GetExpiredFunctions(expiresBefore);

    public Task<IReadOnlyList<FlowInstance>> GetSucceededFunctions(FlowType flowType, long completedBefore)
        => _inner.GetSucceededFunctions(flowType, completedBefore);

    public Task<bool> SetFunctionState(
        FlowId flowId, Status status, 
        string? storedParameter, string? storedResult, 
        StoredException? storedException, 
        long expires, 
        int expectedEpoch
    ) => _inner.SetFunctionState(flowId, status, storedParameter, storedResult, storedException, expires, expectedEpoch);

    public Task<bool> SucceedFunction(
        FlowId flowId, 
        string? result, 
        string? defaultState, 
        long timestamp, 
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _inner.SucceedFunction(flowId, result, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> PostponeFunction(
        FlowId flowId,
        long postponeUntil,
        string? defaultState,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _inner.PostponeFunction(flowId, postponeUntil, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> FailFunction(
        FlowId flowId, 
        StoredException storedException, 
        string? defaultState,
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _inner.FailFunction(flowId, storedException, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> SuspendFunction(
        FlowId flowId,
        long expectedInterruptCount,
        string? defaultState,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _inner.SuspendFunction(flowId, expectedInterruptCount, defaultState, timestamp, expectedEpoch, complimentaryState);

    public Task SetDefaultState(FlowId flowId, string? stateJson)
        => _inner.SetDefaultState(flowId, stateJson); 

    public Task<bool> SetParameters(FlowId flowId, string? storedParameter, string? storedResult, int expectedEpoch)
        => _inner.SetParameters(flowId, storedParameter, storedResult, expectedEpoch);
    
    public Task<bool> IncrementInterruptCount(FlowId flowId)
        => _inner.IncrementInterruptCount(flowId);

    public Task<long?> GetInterruptCount(FlowId flowId)
        => _inner.GetInterruptCount(flowId);

    public Task<StatusAndEpoch?> GetFunctionStatus(FlowId flowId)
        => _inner.GetFunctionStatus(flowId);

    public Task<StoredFlow?> GetFunction(FlowId flowId)
        => _inner.GetFunction(flowId);

    public Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType, Status status)
        => _inner.GetInstances(flowType, status);

    public Task<IReadOnlyList<FlowInstance>> GetInstances(FlowType flowType)
        => _inner.GetInstances(flowType);

    public Task<IReadOnlyList<FlowType>> GetTypes()
        => _inner.GetTypes();

    public Task<bool> DeleteFunction(FlowId flowId) => _inner.DeleteFunction(flowId);
}