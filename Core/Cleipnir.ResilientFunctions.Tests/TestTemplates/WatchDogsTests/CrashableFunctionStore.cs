using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Tests.Messaging.Utils;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.WatchDogsTests;

public class CrashableFunctionStore : IFunctionStore
{
    private readonly IFunctionStore _inner;
    private volatile bool _crashed;
    
    public SyncedFlag AfterPostponeFunctionFlag { get; } = new();
    public ITypeStore TypeStore => _crashed ? throw new TimeoutException() : _inner.TypeStore;
    public IMessageStore MessageStore => _crashed ? throw new TimeoutException() : _inner.MessageStore;

    public IReplicaStore ReplicaStore => _crashed ? throw new TimeoutException() : _inner.ReplicaStore;

    public CrashableFunctionStore(IFunctionStore inner)
    {
        _inner = inner;
    }

    public void Crash() => _crashed = true;

    public void FixCrash() => _crashed = false;

    public Task Initialize() => Task.CompletedTask;

    public Task<IStorageSession?> CreateFunction(
        StoredId storedId,
        FlowInstance humanInstanceId,
        byte[]? param,
        long? postponeUntil,
        long timestamp,
        StoredId? parent,
        ReplicaId? owner,
        IReadOnlyList<StoredEffect>? effects = null
    ) => _crashed
        ? Task.FromException<IStorageSession?>(new TimeoutException())
        : _inner.CreateFunction(
            storedId,
            humanInstanceId,
            param,
            postponeUntil,
            timestamp,
            parent,
            owner,
            effects
        );

    public Task<int> BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
        => _crashed
            ? Task.FromException<int>(new TimeoutException())
            : _inner.BulkScheduleFunctions(functionsWithParam, parent);

    public Task<Dictionary<StoredId, StoredFlowWithEffects>> RestartExecutions(IReadOnlyList<StoredId> storedIds, ReplicaId owner)
        => _crashed
            ? Task.FromException<Dictionary<StoredId, StoredFlowWithEffects>>(new TimeoutException())
            : _inner.RestartExecutions(storedIds, owner);

    public Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long isEligibleBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredId>>(new TimeoutException())
            : _inner.GetExpiredFunctions(isEligibleBefore);

    public Task<IReadOnlyList<StoredId>> GetSucceededFunctions(long completedBefore)
        => _crashed
            ? Task.FromException<IReadOnlyList<StoredId>>(new TimeoutException())
            : _inner.GetSucceededFunctions(completedBefore);

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

    public async Task<bool> SetStatus(
        StoredId storedId,
        Status status,
        byte[]? result,
        StoredException? storedException,
        long expires,
        long timestamp,
        ReplicaId expectedReplica,
        IStorageSession? storageSession)
    {
        if (_crashed)
            throw new TimeoutException();

        var success = await _inner.SetStatus(storedId, status, result, storedException, expires, timestamp, expectedReplica, storageSession);
        if (status == Status.Postponed)
            AfterPostponeFunctionFlag.Raise();

        return success;
    }

    public Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas()
        => _crashed
            ? Task.FromException<IReadOnlyList<ReplicaId>>(new TimeoutException())
            : _inner.GetOwnerReplicas();    

    public Task RescheduleCrashedFunctions(ReplicaId replicaId)
        => _crashed 
            ? Task.FromException(new TimeoutException())
            : _inner.RescheduleCrashedFunctions(replicaId);

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
    
    public Task<bool> DeleteFunction(StoredId storedId)
        => _crashed
            ? Task.FromException<bool>(new TimeoutException())
            : _inner.DeleteFunction(storedId);

    public Task<IReadOnlyDictionary<StoredId, byte[]?>> GetResults(IEnumerable<StoredId> storedIds)
        => _crashed
            ? Task.FromException<IReadOnlyDictionary<StoredId, byte[]?>>(new TimeoutException())
            : _inner.GetResults(storedIds);

    public Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, IStorageSession? session)
        => _crashed
            ? Task.FromException(new TimeoutException())
            : _inner.SetEffectResults(storedId, changes, session);

    public IFunctionStore WithPrefix(string prefix) => _inner.WithPrefix(prefix);
}

public static class CrashableFunctionStoreExtensions
{
    public static CrashableFunctionStore ToCrashableFunctionStore(this IFunctionStore store) => new(store);
}