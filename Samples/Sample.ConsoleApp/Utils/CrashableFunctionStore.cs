using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace ConsoleApp.Utils;

public class CrashableFunctionStore : IFunctionStore
{
    private readonly IFunctionStore _inner;
    private volatile bool _crashed;
    
    public ITypeStore TypeStore => _inner.TypeStore;
    public IMessageStore MessageStore => _inner.MessageStore;
    public IEffectsStore EffectsStore => _inner.EffectsStore;
    public ICorrelationStore CorrelationStore => _inner.CorrelationStore;
    public Utilities Utilities => _inner.Utilities;
    public ISemaphoreStore SemaphoreStore => _inner.SemaphoreStore;
    public IReplicaStore ReplicaStore => _inner.ReplicaStore;

    public CrashableFunctionStore(IFunctionStore inner) => _inner = inner;

    public void Crash() => _crashed = true;

    public Task Initialize() => Task.CompletedTask;

    public Task<IStorageSession?> CreateFunction(
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
        ? Task.FromException<IStorageSession?>(new TimeoutException())
        : _inner.CreateFunction(
            storedId,
            humanInstanceId,
            param,
            leaseExpiration,
            postponeUntil,
            timestamp,
            parent,
            owner,
            effects,
            messages
        );

    public Task BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.BulkScheduleFunctions(functionsWithParam, parent);

    public Task<StoredFlowWithEffectsAndMessages?> RestartExecution(StoredId storedId, ReplicaId replicaId)
        => _crashed
            ? Task.FromException<StoredFlowWithEffectsAndMessages?>(new TimeoutException())
            : _inner.RestartExecution(storedId, replicaId);

    public Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long expiresBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredId>>(new TimeoutException())
            : _inner.GetExpiredFunctions(expiresBefore);

    public Task<IReadOnlyList<StoredId>> GetSucceededFunctions(long completedBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredId>>(new TimeoutException())
            : _inner.GetSucceededFunctions(completedBefore);

    public Task<IReadOnlyList<StoredId>> GetInterruptedFunctions(IEnumerable<StoredId> ids)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredId>>(new TimeoutException())
            : _inner.GetInterruptedFunctions(ids);

    public Task<bool> SetParameters(StoredId storedId, byte[]? param, byte[]? result, ReplicaId? expectedReplica)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SetParameters(storedId, param, result, expectedReplica);

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
        IStorageSession? storageSession
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.SucceedFunction(storedId, result, timestamp, expectedReplica, effects, messages, storageSession);

    public Task<bool> PostponeFunction(
        StoredId storedId,
        long postponeUntil,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.PostponeFunction(storedId, postponeUntil, timestamp, expectedReplica, effects, messages, storageSession); 

    public Task<bool> FailFunction(
        StoredId storedId,
        StoredException storedException,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession
    ) => _crashed
        ? Task.FromException<bool>(new TimeoutException())
        : _inner.FailFunction(storedId, storedException, timestamp, expectedReplica, effects, messages, storageSession);

    public Task<bool> SuspendFunction(
        StoredId storedId,
        long timestamp,
        ReplicaId expectedReplica,
        IReadOnlyList<StoredEffect>? effects,
        IReadOnlyList<StoredMessage>? messages,
        IStorageSession? storageSession
    ) => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.SuspendFunction(storedId, timestamp, expectedReplica, effects, messages, storageSession);

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

    public Task Interrupt(IReadOnlyList<StoredId> storedIds)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.Interrupt(storedIds);

    public Task<bool?> Interrupted(StoredId storedId)
        => _crashed
            ? Task.FromException<bool?>(new TimeoutException())
            : _inner.Interrupted(storedId);
    
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

    public Task<bool> DeleteFunction(StoredId storedId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.DeleteFunction(storedId);

    public IFunctionStore WithPrefix(string prefix)
        => _inner.WithPrefix(prefix);
}