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
    public delegate int LeaseUpdaterCallback(IReadOnlyList<LeaseUpdate> leaseUpdates, long leaseExpiration);

    private readonly LeaseUpdaterCallback _leaseUpdaterCallback;
    private readonly IFunctionStore _inner = new InMemoryFunctionStore();

    public LeaseUpdaterTestFunctionStore(LeaseUpdaterCallback leaseUpdaterCallback) => _leaseUpdaterCallback = leaseUpdaterCallback;

    public ITypeStore TypeStore => _inner.TypeStore;
    public IMessageStore MessageStore => _inner.MessageStore;
    public IEffectsStore EffectsStore => _inner.EffectsStore;
    public ICorrelationStore CorrelationStore => _inner.CorrelationStore;
    public Utilities Utilities => _inner.Utilities;
    public ISemaphoreStore SemaphoreStore => _inner.SemaphoreStore;
    public IReplicaStore ReplicaStore => _inner.ReplicaStore;
    public Task Initialize() => _inner.Initialize();

    public Task<bool> CreateFunction(
        StoredId storedId, 
        FlowInstance humanInstanceId,
        byte[]? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent,
        ReplicaId? owner,
        IReadOnlyList<StoredEffect>? effects = null, 
        IReadOnlyList<StoredMessage>? messages = null
    ) => _inner.CreateFunction(storedId, humanInstanceId, param, leaseExpiration, postponeUntil, timestamp, parent, owner);

    public Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
        => _inner.BulkScheduleFunctions(functionsWithParam, parent);

    public Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration, ReplicaId replicaId)
        => _inner.RestartExecution(storedId, expectedEpoch, leaseExpiration, replicaId);
    
    public Task<int> RenewLeases(IReadOnlyList<LeaseUpdate> leaseUpdates, long leaseExpiration)
        => _leaseUpdaterCallback(leaseUpdates, leaseExpiration).ToTask();

    public Task<IReadOnlyList<IdAndEpoch>> GetExpiredFunctions(long expiresBefore)
        => _inner.GetExpiredFunctions(expiresBefore);

    public Task<IReadOnlyList<StoredInstance>> GetSucceededFunctions(StoredType storedType, long completedBefore)
        => _inner.GetSucceededFunctions(storedType, completedBefore);

    public Task<bool> SetFunctionState(
        StoredId storedId, Status status, 
        byte[]? storedParameter, byte[]? storedResult, 
        StoredException? storedException, 
        long expires, 
        int expectedEpoch
    ) => _inner.SetFunctionState(storedId, status, storedParameter, storedResult, storedException, expires, expectedEpoch);

    public Task<bool> SucceedFunction(
        StoredId storedId, 
        byte[]? result, 
        long timestamp, 
        int expectedEpoch,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    ) => _inner.SucceedFunction(storedId, result, timestamp, expectedEpoch, effects, messages, complimentaryState);

    public Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        bool ignoreInterrupted,
        int expectedEpoch,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    ) => _inner.PostponeFunction(storedId, postponeUntil, timestamp, ignoreInterrupted, expectedEpoch, effects, messages, complimentaryState);

    public Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch, 
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    ) => _inner.FailFunction(storedId, storedException, timestamp, expectedEpoch, effects, messages, complimentaryState);

    public Task<bool> SuspendFunction(
        StoredId storedId,
        long timestamp,
        int expectedEpoch,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    ) => _inner.SuspendFunction(storedId, timestamp, expectedEpoch, effects, messages, complimentaryState);

    public Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas()
        => _inner.GetOwnerReplicas();

    public Task RescheduleCrashedFunctions(ReplicaId replicaId)
        => _inner.RescheduleCrashedFunctions(replicaId);

    public Task<bool> Interrupt(StoredId storedId)
        => _inner.Interrupt(storedId);

    public Task Interrupt(IReadOnlyList<StoredId> storedIds) => _inner.Interrupt(storedIds);

    public Task<bool?> Interrupted(StoredId storedId) => _inner.Interrupted(storedId);

    public Task<bool> SetParameters(StoredId storedId, byte[]? storedParameter, byte[]? storedResult, int expectedEpoch)
        => _inner.SetParameters(storedId, storedParameter, storedResult, expectedEpoch);

    public Task<StatusAndEpoch?> GetFunctionStatus(StoredId storedId)
        => _inner.GetFunctionStatus(storedId);

    public Task<IReadOnlyList<StatusAndEpochWithId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
        => _inner.GetFunctionsStatus(storedIds);

    public Task<StoredFlow?> GetFunction(StoredId storedId)
        => _inner.GetFunction(storedId);

    public Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType, Status status)
        => _inner.GetInstances(storedType, status);

    public Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType storedType)
        => _inner.GetInstances(storedType);

    public Task<bool> DeleteFunction(StoredId storedId) => _inner.DeleteFunction(storedId);
    public IFunctionStore WithPrefix(string prefix) => _inner.WithPrefix(prefix);
}