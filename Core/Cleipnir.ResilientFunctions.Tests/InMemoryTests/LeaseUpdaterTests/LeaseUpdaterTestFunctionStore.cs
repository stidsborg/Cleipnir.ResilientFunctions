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
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public ICorrelationStore CorrelationStore => _inner.CorrelationStore;
    public Utilities Utilities => _inner.Utilities;
    public IMigrator Migrator => _inner.Migrator;
    public ISemaphoreStore SemaphoreStore => _inner.SemaphoreStore;
    public Task Initialize() => _inner.Initialize();

    public Task<bool> CreateFunction(
        StoredId storedId, 
        FlowInstance humanInstanceId,
        byte[]? param, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp,
        StoredId? parent
    ) => _inner.CreateFunction(storedId, humanInstanceId, param, leaseExpiration, postponeUntil, timestamp, parent);

    public Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
        => _inner.BulkScheduleFunctions(functionsWithParam, parent);

    public Task<StoredFlow?> RestartExecution(StoredId storedId, int expectedEpoch, long leaseExpiration)
        => _inner.RestartExecution(storedId, expectedEpoch, leaseExpiration);
    
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
        ComplimentaryState complimentaryState
    ) => _inner.SucceedFunction(storedId, result, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        bool ignoreInterrupted,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _inner.PostponeFunction(storedId, postponeUntil, timestamp, ignoreInterrupted, expectedEpoch, complimentaryState);

    public Task<bool> FailFunction(
        StoredId storedId, 
        StoredException storedException, 
        long timestamp,
        int expectedEpoch, 
        ComplimentaryState complimentaryState
    ) => _inner.FailFunction(storedId, storedException, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> SuspendFunction(
        StoredId storedId,
        long timestamp,
        int expectedEpoch,
        ComplimentaryState complimentaryState
    ) => _inner.SuspendFunction(storedId, timestamp, expectedEpoch, complimentaryState);

    public Task<bool> Interrupt(StoredId storedId, bool onlyIfExecuting)
        => _inner.Interrupt(storedId, onlyIfExecuting);

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
}