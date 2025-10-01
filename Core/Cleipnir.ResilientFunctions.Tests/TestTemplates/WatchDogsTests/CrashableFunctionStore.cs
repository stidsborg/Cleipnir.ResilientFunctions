using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public class CrashableFunctionStore : IFunctionStore
{
    private readonly IFunctionStore _inner;
    private volatile bool _crashed;
    
    public SyncedFlag AfterPostponeFunctionFlag { get; } = new();
    public ITypeStore TypeStore => _crashed ? throw new TimeoutException() : _inner.TypeStore;
    public IMessageStore MessageStore => _crashed ? throw new TimeoutException() : _inner.MessageStore;

    private readonly CrashableEffectStore _crashableEffectStore;
    public IEffectsStore EffectsStore => _crashableEffectStore;
    public ICorrelationStore CorrelationStore => _crashed ? throw new TimeoutException() : _inner.CorrelationStore;
    public Utilities Utilities => _crashed ? throw new TimeoutException() : _inner.Utilities;
    public ISemaphoreStore SemaphoreStore => _crashed ? throw new TimeoutException() : _inner.SemaphoreStore;
    public IReplicaStore ReplicaStore => _crashed ? throw new TimeoutException() : _inner.ReplicaStore;

    public CrashableFunctionStore(IFunctionStore inner)
    {
        _inner = inner;
        _crashableEffectStore = new CrashableEffectStore(inner.EffectsStore);
    }

    public void Crash()
    {
        _crashed = true;
        _crashableEffectStore.Crashed = true;
    }

    public void FixCrash()
    {
        _crashed = false;
        _crashableEffectStore.Crashed = false;
    } 

    public Task Initialize() => Task.CompletedTask;

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
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.CreateFunction(
            storedId,
            humanInstanceId,
            param,
            leaseExpiration,
            postponeUntil,
            timestamp,
            parent,
            owner
        );

    public Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.BulkScheduleFunctions(functionsWithParam, parent);

    public Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, ReplicaId replicaId)
        => _crashed
            ? Task.FromException<StoredFlowWithEffectsAndMessages?>(new TimeoutException())
            : _inner.RestartExecution(storedId, replicaId);

    public Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long isEligibleBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredId>>(new TimeoutException())
            : _inner.GetExpiredFunctions(isEligibleBefore);

    public Task<IReadOnlyList<StoredId>> GetSucceededFunctions(StoredType storedType, long completedBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredId>>(new TimeoutException())
            : _inner.GetSucceededFunctions(storedType, completedBefore);

    public Task<bool> SetFunctionState(
        StoredId storedId, Status status, byte[]? storedParameter,
        byte[]? storedResult, StoredException? storedException, 
        long expires, 
        ReplicaId? expectedReplica
    ) => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetFunctionState(
                storedId, status, 
                storedParameter, storedResult, 
                storedException, expires, 
                expectedReplica
            );

    public Task<bool> SucceedFunction(
        StoredId storedId,
        byte[]? result,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.SucceedFunction(storedId, result, timestamp, expectedReplica, effects, messages, complimentaryState);

    public async Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        bool ignoreInterrupted,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    )
    {
        if (_crashed)
            throw new TimeoutException();

        var result = await _inner.PostponeFunction(storedId, postponeUntil, timestamp, ignoreInterrupted, expectedReplica, effects, messages, complimentaryState);
        AfterPostponeFunctionFlag.Raise();

        return result;
    } 

    public Task<bool> FailFunction(
        StoredId storedId,
        StoredException storedException,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.FailFunction(storedId, storedException, timestamp, expectedReplica, effects, messages, complimentaryState);

    public Task<bool> SuspendFunction(
        StoredId storedId, 
        long timestamp, 
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        ComplimentaryState complimentaryState)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SuspendFunction(storedId, timestamp, expectedReplica, effects, messages, complimentaryState);

    public Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas()
        => _crashed
            ? Task.FromException<IReadOnlyList<ReplicaId>>(new TimeoutException())
            : _inner.GetOwnerReplicas();    

    public Task RescheduleCrashedFunctions(ReplicaId replicaId)
        => _crashed 
            ? Task.FromException(new TimeoutException())
            : _inner.RescheduleCrashedFunctions(replicaId);

    public Task<bool> Interrupt(StoredId storedId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.Interrupt(storedId);

    public Task Interrupt(IReadOnlyList<StoredId> storedIds) => _inner.Interrupt(storedIds);

    public Task<bool?> Interrupted(StoredId storedId) 
        => _crashed
            ? Task.FromException<bool?>(new TimeoutException())
            : _inner.Interrupted(storedId);
    
    public Task<bool> SetParameters(StoredId storedId, byte[]? storedParameter, byte[]? storedResult, ReplicaId? expectedReplica)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetParameters(storedId, storedParameter, storedResult, expectedReplica);
    
    public Task<Status?> GetFunctionStatus(StoredId storedId)
        => _crashed
            ? Task.FromException<Status?>(new TimeoutException())
            : _inner.GetFunctionStatus(storedId);

    public Task<IReadOnlyList<StatusAndId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
        => _crashed
            ? Task.FromException<IReadOnlyList<StatusAndId>>(new TimeoutException())
            : _inner.GetFunctionsStatus(storedIds);

    public Task<StoredFlow?> GetFunction(StoredId storedId)
        => _crashed
            ? Task.FromException<StoredFlow?>(new TimeoutException())
            : _inner.GetFunction(storedId);

    public Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType flowType, Status status)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredInstance>>(new TimeoutException())
            : _inner.GetInstances(flowType, status);

    public Task<IReadOnlyList<StoredInstance>> GetInstances(StoredType flowType)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredInstance>>(new TimeoutException())
            : _inner.GetInstances(flowType);

    public Task<bool> DeleteFunction(StoredId storedId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.DeleteFunction(storedId);

    public IFunctionStore WithPrefix(string prefix) => _inner.WithPrefix(prefix);
}

public static class CrashableFunctionStoreExtensions
{
    public static CrashableFunctionStore ToCrashableFunctionStore(this IFunctionStore store) => new(store);
}