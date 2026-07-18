using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

[TestClass]
public class EffectFlushConcurrencyTests
{
    private static Effect CreateEffect(StoredId storedId, IFunctionStore functionStore)
    {
        var effectResults = new EffectResults(
            TestFlowId.Create(),
            storedId,
            existingEffects: new List<StoredEffect>(),
            functionStore,
            DefaultSerializer.Instance,
            owner: null,
            storageSession: null,
            clearChildren: true
        );

        return new Effect(
            effectResults,
            utcNow: () => DateTime.UtcNow,
            new FlowTimeouts(),
            new FlowExecutionState(storedId, subflows: 1, waitingSubflows: 0, new FlowTimeouts(), completed: ForeverTask.Instance)
        );
    }

    [TestMethod]
    public async Task UpsertDuringFlushStoreWriteStaysPendingAndIsPersistedByNextFlush()
    {
        var inner = new InMemoryFunctionStore();
        var store = new GatedEffectWritesFunctionStore(inner);
        var storedId = TestStoredId.Create();
        await inner.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        var effect = CreateEffect(storedId, store);
        var id = new EffectId([1]);

        effect.FlushlessUpsert(id, "v1", alias: null);

        store.Gate();
        var flushTask = effect.Flush();
        await store.EnteredWrite;

        // Lands while the flush's store write is in flight - it was not part of the flushed snapshot.
        effect.FlushlessUpsert(id, "v2", alias: null);

        store.Release();
        await flushTask;

        effect.IsDirty(id).ShouldBeTrue();

        await effect.Flush();
        var storedEffect = (await inner.GetFunction(storedId))!.Effects!.Single(e => e.EffectId == id);
        DefaultSerializer.Instance.Deserialize(storedEffect.Result!, typeof(string)).ShouldBe("v2");
    }

    [TestMethod]
    public async Task RecreateDuringFlushedDeleteStaysPendingAndIsPersistedByNextFlush()
    {
        var inner = new InMemoryFunctionStore();
        var store = new GatedEffectWritesFunctionStore(inner);
        var storedId = TestStoredId.Create();
        await inner.CreateFunction(storedId, "humanInstanceId", param: null, postponeUntil: null, timestamp: DateTime.UtcNow.Ticks, parent: null, owner: null);

        var effect = CreateEffect(storedId, store);
        var id = new EffectId([1]);

        effect.FlushlessUpsert(id, "v1", alias: null);
        await effect.Flush();

        effect.FlushlessClear(id);

        store.Gate();
        var flushTask = effect.Flush();
        await store.EnteredWrite;

        // Re-created while the flushed delete's store write is in flight.
        effect.FlushlessUpsert(id, "v2", alias: null);

        store.Release();
        await flushTask;

        effect.IsDirty(id).ShouldBeTrue();

        await effect.Flush();
        var storedEffect = (await inner.GetFunction(storedId))!.Effects!.Single(e => e.EffectId == id);
        DefaultSerializer.Instance.Deserialize(storedEffect.Result!, typeof(string)).ShouldBe("v2");
    }

    private class GatedEffectWritesFunctionStore(IFunctionStore inner) : IFunctionStore
    {
        private volatile TaskCompletionSource? _gate;
        private volatile TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task EnteredWrite => _entered.Task;

        public void Gate()
        {
            _entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void Release()
        {
            _gate?.TrySetResult();
            _gate = null;
        }

        public async Task SetEffectResults(StoredId storedId, IReadOnlyList<StoredEffectChange> changes, ReplicaId? owner, IStorageSession? session)
        {
            var gate = _gate;
            if (gate != null)
            {
                _entered.TrySetResult();
                await gate.Task;
            }

            await inner.SetEffectResults(storedId, changes, owner, session);
        }

        public ITypeStore TypeStore => inner.TypeStore;
        public IMessageStore MessageStore => inner.MessageStore;
        public IReplicaStore ReplicaStore => inner.ReplicaStore;
        public Task Initialize() => inner.Initialize();

        public Task<IStorageSession?> CreateFunction(StoredId storedId, FlowInstance humanInstanceId, byte[]? param, long? postponeUntil, long timestamp, StoredId? parent, ReplicaId? owner, IReadOnlyList<StoredEffect>? effects = null)
            => inner.CreateFunction(storedId, humanInstanceId, param, postponeUntil, timestamp, parent, owner, effects);

        public Task<int> BulkScheduleFunctions(IEnumerable<IdWithParam> functionsWithParam, StoredId? parent)
            => inner.BulkScheduleFunctions(functionsWithParam, parent);

        public Task<Dictionary<StoredId, StoredFlowWithEffects>> RestartExecutions(IReadOnlyList<StoredId> storedIds, ReplicaId owner)
            => inner.RestartExecutions(storedIds, owner);

        public Task<IReadOnlyList<StoredId>> GetExpiredFunctions(long isEligibleBefore)
            => inner.GetExpiredFunctions(isEligibleBefore);

        public Task<IReadOnlyList<StoredId>> GetSucceededFunctions(long completedBefore)
            => inner.GetSucceededFunctions(completedBefore);

        public Task<bool> SetFunctionState(StoredId storedId, Status status, byte[]? storedParameter, byte[]? storedResult, StoredException? storedException, long expires, ReplicaId? expectedReplica)
            => inner.SetFunctionState(storedId, status, storedParameter, storedResult, storedException, expires, expectedReplica);

        public Task<bool> SetStatus(StoredId storedId, Status status, byte[]? result, StoredException? storedException, long expires, long timestamp, ReplicaId expectedReplica, IStorageSession? storageSession)
            => inner.SetStatus(storedId, status, result, storedException, expires, timestamp, expectedReplica, storageSession);

        public Task<IReadOnlyList<ReplicaId>> GetOwnerReplicas() => inner.GetOwnerReplicas();
        public Task RescheduleCrashedFunctions(ReplicaId replicaId) => inner.RescheduleCrashedFunctions(replicaId);

        public Task<bool> SetParameters(StoredId storedId, byte[]? storedParameter, byte[]? storedResult, ReplicaId? expectedReplica)
            => inner.SetParameters(storedId, storedParameter, storedResult, expectedReplica);

        public Task<Status?> GetFunctionStatus(StoredId storedId) => inner.GetFunctionStatus(storedId);

        public Task<IReadOnlyList<StatusAndId>> GetFunctionsStatus(IEnumerable<StoredId> storedIds)
            => inner.GetFunctionsStatus(storedIds);

        public Task<StoredFlow?> GetFunction(StoredId storedId) => inner.GetFunction(storedId);
        public Task<bool> DeleteFunction(StoredId storedId) => inner.DeleteFunction(storedId);

        public Task<IReadOnlyDictionary<StoredId, byte[]?>> GetResults(IEnumerable<StoredId> storedIds)
            => inner.GetResults(storedIds);

        public IFunctionStore WithPrefix(string prefix) => inner.WithPrefix(prefix);
    }
}
