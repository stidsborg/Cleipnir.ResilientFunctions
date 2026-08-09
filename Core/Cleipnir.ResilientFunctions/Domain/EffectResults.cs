using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.Domain;

internal class EffectResults
{
    private readonly FlowId _flowId;
    private readonly StoredId _storedId;
    private readonly IReadOnlyList<DeserializedEffect> _existingEffects;
    private readonly IFunctionStore _functionStore;
    private readonly ISerializer _serializer;
    private readonly TypeMapper _typeMapper;
    private readonly ReplicaId? _owner;
    private readonly IStorageSession? _storageSession;
    private readonly bool _clearChildren;

    private readonly Lock _sync = new();
    private volatile bool _initialized;

    private readonly Dictionary<EffectId, PendingEffectChange> _effectResults = new();

    public Dictionary<EffectId, PendingEffectChange> Results
    {
        get
        {
            lock (_sync)
                return _effectResults.ToDictionary(kv => kv.Key, kv => kv.Value);
        }
    }

    public QueueManager? QueueManager { get; set; }
    
    public EffectResults(
        FlowId flowId,
        StoredId storedId,
        IReadOnlyList<DeserializedEffect> existingEffects,
        IFunctionStore functionStore,
        ISerializer serializer,
        TypeMapper typeMapper,
        ReplicaId? owner,
        IStorageSession? storageSession,
        bool clearChildren)
    {
        _flowId = flowId;
        _storedId = storedId;
        _existingEffects = existingEffects;
        _functionStore = functionStore;
        _serializer = serializer;
        _typeMapper = typeMapper;
        _owner = owner;
        _storageSession = storageSession;
        _clearChildren = clearChildren;

        Initialize();
    }
    
    public EffectId? GetEffectId(string alias)
    {
        lock (_sync)
            return _effectResults
                .Values
                .FirstOrDefault(c => c.Alias == alias)
                ?.Id;
    }

    public IEnumerable<EffectId> EffectIds => _effectResults.Keys.ToList();
    
    private void Initialize()
    {
        if (_initialized)
            return;
        
        lock (_sync)
        {
            if (_initialized)
                return;

            foreach (var existingEffect in _existingEffects)
                _effectResults[existingEffect.EffectId] =
                    new PendingEffectChange(
                        existingEffect.EffectId,
                        existingEffect,
                        StoredEffect: null,
                        Operation: null,
                        Existing: true,
                        existingEffect.Alias
                    );
            
            _initialized = true;
        }
    }

    public bool Contains(EffectId effectId)
    {
        lock (_sync)
            return _effectResults.ContainsKey(effectId);
    }

    public DeserializedEffect? GetOrValueDefault(EffectId effectId)
    {
        lock (_sync)
            return _effectResults.GetValueOrDefault(effectId)?.Effect;
    }

    public async Task Set(EffectId effectId, string? alias, bool flush)
    {
        await FlushOrAddToPending(
            effectId,
            new DeserializedEffect(effectId, WorkStatus.Completed, Result: null, StoredException: null, alias),
            StoredEffect.CreateCompleted(effectId, alias),
            flush,
            delete: false,
            clearChildren: false
        );
    }

    public async Task<T> CreateOrGet<T>(EffectId effectId, T value, string? alias, bool flush)
    {
        DeserializedEffect? existing;
        lock (_sync)
            existing = _effectResults.GetValueOrDefault(effectId)?.Effect;

        if (existing?.WorkStatus == WorkStatus.Completed)
            return (T)existing.Result!;

        if (existing?.StoredException != null)
            throw FatalWorkflowException.Create(_flowId, existing.StoredException!);

        var (valueToSerialize, valueType) = EffectValue.ForSerialization(value, typeof(T));
        var serializedValue = _serializer.Serialize(valueToSerialize!, valueType);
        var storedEffect = StoredEffect.CreateCompleted(effectId, serializedValue, _typeMapper.GetTypeId(valueType), alias);
        await FlushOrAddToPending(
            effectId,
            new DeserializedEffect(effectId, WorkStatus.Completed, valueToSerialize, StoredException: null, alias),
            storedEffect,
            flush,
            delete: false,
            clearChildren: false
        );

        return value;
    }

    internal async Task Upsert<T>(EffectId effectId, string? alias, T value, bool flush)
    {
        FlushlessUpsert(effectId, alias, value);

        if (flush)
            await Flush();
    }

    internal void FlushlessUpsert<T>(EffectId effectId, string? alias, T value)
    {
        var (valueToSerialize, valueType) = EffectValue.ForSerialization(value, typeof(T));
        var serializedValue = _serializer.Serialize(valueToSerialize!, valueType);
        var storedEffect = StoredEffect.CreateCompleted(effectId, serializedValue, _typeMapper.GetTypeId(valueType), alias);
        AddToPending(
            effectId,
            new DeserializedEffect(effectId, WorkStatus.Completed, valueToSerialize, StoredException: null, alias),
            storedEffect,
            delete: false,
            clearChildren: false
        );
    }

    internal async Task Upserts(IEnumerable<EffectResult> values, bool flush)
    {
        FlushlessUpserts(values);

        if (flush)
            await Flush();
    }

    // The batch enters the pending set under a single lock acquisition, so a concurrent flush snapshot sees
    // either none or all of its entries - upserts and clears (EffectResult.Delete) can never be persisted
    // torn across two flushes.
    internal void FlushlessUpserts(IEnumerable<EffectResult> values)
    {
        var changes = values
            .Select(t =>
            {
                var (value, valueType) = EffectValue.ForSerialization(t.Value, typeof(object));
                return new
                {
                    Id = t.Id,
                    Effect = t.Delete
                        ? null
                        : new DeserializedEffect(t.Id, WorkStatus.Completed, value, StoredException: null, t.Alias),
                    StoredEffect = t.Delete
                        ? null
                        : StoredEffect.CreateCompleted(
                            t.Id,
                            _serializer.Serialize(value!, valueType),
                            _typeMapper.GetTypeId(valueType),
                            t.Alias
                        ),
                    Delete = t.Delete
                };
            })
            .ToList();

        lock (_sync)
            foreach (var change in changes)
                if (change.Delete)
                {
                    if (_effectResults.ContainsKey(change.Id))
                        AddToPending(change.Id, effect: null, storedEffect: null, delete: true, clearChildren: true);
                }
                else
                    AddToPending(change.Id, change.Effect, change.StoredEffect, delete: false, clearChildren: false);
    }
    
    public (bool Success, T? Value) TryGet<T>(EffectId effectId)
    {
        DeserializedEffect? effect;
        lock (_sync)
            effect = _effectResults.GetValueOrDefault(effectId)?.Effect;

        if (effect?.WorkStatus == WorkStatus.Completed)
            return (true, (T?)effect.Result);

        if (effect?.StoredException != null)
            throw FatalWorkflowException.Create(_flowId, effect.StoredException!);

        return (false, default);
    }
    
    public IReadOnlyList<EffectId> GetChildren(EffectId parentId)
    {
        lock (_sync)
            return _effectResults
                .Keys
                .Where(id => id.IsDescendant(parentId))
                .ToList();
    }

    public async Task InnerCapture(EffectId effectId, string? alias, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext)
    {
        EffectContext.SetParent(effectId);

        lock (_sync)
        {
            var success = _effectResults.TryGetValue(effectId, out var pendingChange);
            var effect = pendingChange?.Effect;
            if (success && effect?.WorkStatus == WorkStatus.Completed)
                return;
            if (success && effect?.WorkStatus == WorkStatus.Failed)
                throw FatalWorkflowException.Create(_flowId, effect.StoredException!);
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{effectId}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            await FlushOrAddToPending(
                effectId,
                new DeserializedEffect(effectId, WorkStatus.Started, Result: null, StoredException: null, alias),
                StoredEffect.CreateStarted(effectId, alias),
                flush: true,
                delete: false,
                clearChildren: false
            );
        }

        try
        {
            await work();
        }
        catch (FatalWorkflowException exception)
        {
            var storedException = exception.ToStoredException();
            await FlushOrAddToPending(
                effectId,
                new DeserializedEffect(effectId, WorkStatus.Failed, Result: null, storedException, alias),
                StoredEffect.CreateFailed(effectId, storedException, alias),
                flush: true,
                delete: false,
                clearChildren: false
            );

            exception.FlowId = _flowId;
            throw;
        }
        catch (Exception exception)
        {
            var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(_flowId, exception);
            var storedException = fatalWorkflowException.ToStoredException();
            await FlushOrAddToPending(
                effectId,
                new DeserializedEffect(effectId, WorkStatus.Failed, Result: null, storedException, alias),
                StoredEffect.CreateFailed(effectId, storedException, alias),
                flush: true,
                delete: false,
                clearChildren: false
            );

            throw fatalWorkflowException;
        }

        await FlushOrAddToPending(
            effectId,
            new DeserializedEffect(effectId, WorkStatus.Completed, Result: null, StoredException: null, alias),
            StoredEffect.CreateCompleted(effectId, alias),
            flush: resiliency != ResiliencyLevel.AtLeastOnceDelayFlush,
            delete: false,
            clearChildren: _clearChildren
        );
    }
    
    public async Task<T> InnerCapture<T>(EffectId effectId, string? alias, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext)
    {
        EffectContext.SetParent(effectId);

        PendingEffectChange? pendingChange;
        lock (_sync)
            pendingChange = _effectResults.GetValueOrDefault(effectId);

        if (pendingChange != null)
        {
            var effect = pendingChange.Effect;
            if (effect?.WorkStatus == WorkStatus.Completed)
                return (effect.Result == null ? default : (T)effect.Result)!;
            if (effect?.WorkStatus == WorkStatus.Failed)
                throw FatalWorkflowException.Create(_flowId, effect.StoredException!);
            if (resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{effectId}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            await FlushOrAddToPending(
                effectId,
                new DeserializedEffect(effectId, WorkStatus.Started, Result: null, StoredException: null, alias),
                StoredEffect.CreateStarted(effectId, alias),
                flush: true,
                delete: false,
                clearChildren: false
            );
        }

        T result;
        try
        {
            result = await work();
        }
        catch (FatalWorkflowException exception)
        {
            var storedException = exception.ToStoredException();
            await FlushOrAddToPending(
                effectId,
                new DeserializedEffect(effectId, WorkStatus.Failed, Result: null, storedException, alias),
                StoredEffect.CreateFailed(effectId, storedException, alias),
                flush: true,
                delete: false,
                clearChildren: false
            );

            exception.FlowId = _flowId;
            throw;
        }
        catch (Exception exception)
        {
            var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(_flowId, exception);
            var storedException = fatalWorkflowException.ToStoredException();
            await FlushOrAddToPending(
                effectId,
                new DeserializedEffect(effectId, WorkStatus.Failed, Result: null, storedException, alias),
                StoredEffect.CreateFailed(effectId, storedException, alias),
                flush: true,
                delete: false,
                clearChildren: false
            );
            throw fatalWorkflowException;
        }

        {
            var (resultToSerialize, resultType) = EffectValue.ForSerialization(result, typeof(T));
            var serializedResult = _serializer.Serialize(resultToSerialize!, resultType);
            var storedEffect = StoredEffect.CreateCompleted(effectId, serializedResult, _typeMapper.GetTypeId(resultType), alias);
            await FlushOrAddToPending(
                effectId,
                new DeserializedEffect(effectId, WorkStatus.Completed, resultToSerialize, StoredException: null, alias),
                storedEffect,
                flush: resiliency != ResiliencyLevel.AtLeastOnceDelayFlush,
                delete: false,
                clearChildren: _clearChildren
            );

            return result;
        }
    }

    public async Task Clear(EffectId effectId, bool flush)
    {
        FlushlessClear(effectId);

        if (flush)
            await Flush();
    }

    public void FlushlessClear(EffectId effectId)
    {
        lock (_sync)
            if (_effectResults.ContainsKey(effectId))
                AddToPending(
                    effectId,
                    effect: null,
                    storedEffect: null,
                    delete: true,
                    clearChildren: true
                );
    }

    private void AddToPending(EffectId effectId, DeserializedEffect? effect, StoredEffect? storedEffect, bool delete, bool clearChildren)
    {
        lock (_sync)
        {
            if (_effectResults.ContainsKey(effectId))
            {
                var existing = _effectResults[effectId];
                _effectResults[effectId] = existing with
                {
                    Effect = effect,
                    StoredEffect = storedEffect,
                    Operation = delete
                        ? CrudOperation.Delete
                        : (existing.Existing ? CrudOperation.Update : CrudOperation.Insert),
                    Alias = effect?.Alias,
                };
            }
            else
            {
                _effectResults[effectId] = new PendingEffectChange(
                    effectId,
                    effect,
                    storedEffect,
                    CrudOperation.Insert,
                    Existing: false,
                    effect?.Alias
                );
            }

            if (clearChildren)
            {
                var children = _effectResults.Keys.Where(id => id.IsDescendant(effectId));
                foreach (var child in children)
                    _effectResults[child] =
                        _effectResults[child] with { Operation = CrudOperation.Delete };
            }
        }
    }

    private async Task FlushOrAddToPending(EffectId effectId, DeserializedEffect? effect, StoredEffect? storedEffect, bool flush, bool delete, bool clearChildren)
    {
        AddToPending(effectId, effect, storedEffect, delete, clearChildren);

        if (flush)
            await Flush();
    }
    
    private readonly SemaphoreSlim _flushSync = new(initialCount: 1, maxCount: 1);
    public async Task Flush()
    {
        await _flushSync.WaitAsync();

        try
        {
            // Everything the queue manager has written when this returns is included in the snapshot below -
            // BeforeFlush marks the queue manager's watermark: what a completed flush has provably persisted.
            QueueManager?.BeforeFlush();

            IReadOnlyList<PendingEffectChange> pendingChanges;
            lock (_sync)
                pendingChanges = _effectResults.Values.Where(r => r.Operation != null).ToList();

            if (pendingChanges.Count == 0)
                return;

            var changes = pendingChanges
                .Select(p =>
                    new StoredEffectChange(
                        _storedId,
                        p.Id,
                        p.Operation!.Value,
                        p.StoredEffect
                    )
                ).ToList();

            // The types referenced by the batch must exist in the type store before the effects do - otherwise
            // a crash between the two writes would leave payloads that can never be deserialized. Persisting all
            // minted types also covers ids buried inside already-encoded payloads (staged-message children).
            // Completes synchronously when the batch introduces no new types.
            await _typeMapper.EnsurePersisted();

            await _functionStore.SetEffectResults(_storedId, changes, _owner, _storageSession);

            lock (_sync)
                foreach (var pendingChange in pendingChanges)
                {
                    // Only mark clean what was actually persisted: a concurrent flushless write during the store
                    // write above replaces the dictionary entry with a newer record - that newer change was not
                    // part of this flush and must stay pending, not be cleaned (or removed) by id alone.
                    if (!_effectResults.TryGetValue(pendingChange.Id, out var current) || !ReferenceEquals(current, pendingChange))
                        continue;

                    if (pendingChange.Operation == CrudOperation.Delete)
                        _effectResults.Remove(pendingChange.Id);
                    else
                        // The serialized payload is dropped along with the operation: it has served its purpose
                        // (the store write above) - the deserialized form remains as the read view.
                        _effectResults[pendingChange.Id] = pendingChange with
                        {
                            Existing = true,
                            Operation = null,
                            StoredEffect = null
                        };
                }

            // Still under the flush lock, so BeforeFlush/AfterFlush cycles never overlap: a later flush cannot
            // reset the queue manager's watermark before this one has acted on it. Flushless writes are not
            // blocked - they do not take the flush lock.
            await (QueueManager?.AfterFlush() ?? Task.CompletedTask);
        }
        finally
        {
            _flushSync.Release();
        }
    }

    public bool IsDirty(EffectId effectId)
    {
        lock (_sync)
            if (_effectResults.TryGetValue(effectId, out var change))
                return change.Operation != null;
            else
                return false;
    }
}