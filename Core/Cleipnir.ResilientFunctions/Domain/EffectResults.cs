using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.Domain;

public class EffectResults
{
    private readonly FlowId _flowId;
    private readonly StoredId _storedId;
    private readonly IReadOnlyList<StoredEffect> _existingEffects;
    private readonly IEffectsStore _effectsStore;
    private readonly ISerializer _serializer;
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
        IReadOnlyList<StoredEffect> existingEffects,
        IEffectsStore effectsStore,
        ISerializer serializer,
        IStorageSession? storageSession,
        bool clearChildren)
    {
        _flowId = flowId;
        _storedId = storedId;
        _existingEffects = existingEffects;
        _effectsStore = effectsStore;
        _serializer = serializer;
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

    public StoredEffect? GetOrValueDefault(EffectId effectId)
    {
        lock (_sync)
            return _effectResults.GetValueOrDefault(effectId)?.StoredEffect;
    }

    public async Task Set(StoredEffect storedEffect, bool flush)
    {
        await FlushOrAddToPending(
            storedEffect.EffectId,
            storedEffect,
            flush,
            delete: false,
            clearChildren: false
        );
    }

    public async Task<T> CreateOrGet<T>(EffectId effectId, T value, string? alias, bool flush)
    {
        lock (_sync)
        {
            if (_effectResults.TryGetValue(effectId, out var existing) && existing.StoredEffect?.WorkStatus == WorkStatus.Completed)
                return (T)_serializer.Deserialize(existing.StoredEffect.Result!, typeof(T));

            if (existing?.StoredEffect?.StoredException != null)
                throw _serializer.DeserializeException(_flowId, existing.StoredEffect.StoredException!);
        }

        var storedEffect = StoredEffect.CreateCompleted(effectId, _serializer.Serialize(value), alias);
        await FlushOrAddToPending(
            storedEffect.EffectId,
            storedEffect,
            flush,
            delete: false,
            clearChildren: false
        );

        return value;
    }
    
    internal async Task Upsert<T>(EffectId effectId, string? alias, T value, bool flush)
    {
        var storedEffect = StoredEffect.CreateCompleted(effectId, _serializer.Serialize(value), alias);
        await FlushOrAddToPending(
            storedEffect.EffectId,
            storedEffect,
            flush,
            delete: false,
            clearChildren: false
        );
    }
    
    internal async Task Upserts(IEnumerable<EffectResult> values, bool flush)
    {
        var storedEffects = values
            .Select(t => new { Id = t.Id, Bytes = _serializer.Serialize(t.Value, t.Value?.GetType() ?? typeof(object)), Alias = t.Alias })
            .Select(a => StoredEffect.CreateCompleted(a.Id, a.Bytes, a.Alias))
            .ToList();

        AddToPending(storedEffects);

        if (flush)
            await Flush();
    }
    
    public bool TryGet<T>(EffectId effectId, out T? value)
    {
        lock (_sync)
        {
            if (_effectResults.TryGetValue(effectId, out var change))
            {
                var storedEffect = change.StoredEffect;
                if (storedEffect?.WorkStatus == WorkStatus.Completed)
                {
                    value = (T?)_serializer.Deserialize(storedEffect.Result!, typeof(T));
                    return true;
                }

                if (storedEffect?.StoredException != null)
                    throw _serializer.DeserializeException(_flowId, storedEffect.StoredException!);
            }
        }

        value = default;
        return false;
    }
    
    public Option<object?> TryGet(EffectId effectId, Type type)
    {
        lock (_sync)
        {
            if (_effectResults.TryGetValue(effectId, out var change))
            {
                var storedEffect = change.StoredEffect;
                if (storedEffect?.WorkStatus == WorkStatus.Completed)
                {
                    var value = _serializer.Deserialize(storedEffect.Result!, type)!;
                    return Option.Create((object?) value);    
                }
                
                if (storedEffect?.StoredException != null)
                    throw _serializer.DeserializeException(_flowId, storedEffect.StoredException!);
            }
        }
        
        return Option<object?>.NoValue;
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
            var storedEffect = pendingChange?.StoredEffect;
            if (success && storedEffect?.WorkStatus == WorkStatus.Completed)
                return;
            if (success && storedEffect?.WorkStatus == WorkStatus.Failed)
                throw _serializer.DeserializeException(_flowId, storedEffect.StoredException!);
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{effectId}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            var storedEffect = StoredEffect.CreateStarted(effectId, alias);
            await FlushOrAddToPending(effectId, storedEffect, flush: true, delete: false, clearChildren: false);
        }

        try
        {
            await work();
        }
        catch (SuspendInvocationException)
        {
            throw;
        }
        catch (FatalWorkflowException exception)
        {
            var storedException = _serializer.SerializeException(exception);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException, alias);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect,
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
            var storedException = _serializer.SerializeException(fatalWorkflowException);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException, alias);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect,
                flush: true,
                delete: false,
                clearChildren: false
            );

            throw fatalWorkflowException;
        }

        {
            var storedEffect = StoredEffect.CreateCompleted(effectId, alias);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect,
                flush: resiliency != ResiliencyLevel.AtLeastOnceDelayFlush,
                delete: false,
                clearChildren: _clearChildren
            );
        }
    }
    
    public async Task<T> InnerCapture<T>(EffectId effectId, string? alias, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext)
    {
        EffectContext.SetParent(effectId);

        lock (_sync)
        {
            var success = _effectResults.TryGetValue(effectId, out var storedEffect);
            if (success && storedEffect!.StoredEffect?.WorkStatus == WorkStatus.Completed)
                return (storedEffect.StoredEffect?.Result == null ? default : (T) _serializer.Deserialize(storedEffect.StoredEffect?.Result!, typeof(T)))!;
            if (success && storedEffect!.StoredEffect?.WorkStatus == WorkStatus.Failed)
                throw FatalWorkflowException.Create(_flowId, storedEffect.StoredEffect?.StoredException!);
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{effectId}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            var storedEffect = StoredEffect.CreateStarted(effectId, alias);
            await FlushOrAddToPending(
                effectId,
                storedEffect,
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
        catch (SuspendInvocationException)
        {
            throw;
        }
        catch (FatalWorkflowException exception)
        {
            var storedException = _serializer.SerializeException(exception);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException, alias);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect,
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
            var storedException = _serializer.SerializeException(fatalWorkflowException);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException, alias);

            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect,
                flush: true,
                delete: false,
                clearChildren: false
            );
            throw fatalWorkflowException;
        }

        {
            var storedEffect = StoredEffect.CreateCompleted(effectId, _serializer.Serialize(result), alias);
            await FlushOrAddToPending(
                storedEffect.EffectId,
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
        lock (_sync)
            if (_effectResults.ContainsKey(effectId))
                AddToPending(
                    effectId,
                    storedEffect: null,
                    delete: true,
                    clearChildren: true
                );
        
        if (flush)
            await Flush();
    }
    
    public void ClearNoFlush(EffectId effectId)
    {
        lock (_sync)
            if (_effectResults.ContainsKey(effectId))
                AddToPending(
                    effectId,
                    storedEffect: null,
                    delete: true,
                    clearChildren: true
                );
    }

    private void AddToPending(IEnumerable<StoredEffect> storedEffects)
    {
        lock (_sync)
            foreach (var storedEffect in storedEffects)
                AddToPending(storedEffect.EffectId, storedEffect, delete: false, clearChildren: false);
    }

    private void AddToPending(EffectId effectId, StoredEffect? storedEffect, bool delete, bool clearChildren)
    {
        lock (_sync)
        {
            if (_effectResults.ContainsKey(effectId))
            {
                var existing = _effectResults[effectId];
                _effectResults[effectId] = existing with
                {
                    StoredEffect = storedEffect,
                    Operation = delete 
                        ? CrudOperation.Delete
                        : (existing.Existing ? CrudOperation.Update : CrudOperation.Insert),
                    Alias = storedEffect?.Alias,
                };
            }
            else
            {
                _effectResults[effectId] = new PendingEffectChange(
                    effectId,
                    storedEffect,
                    CrudOperation.Insert,
                    Existing: false,
                    storedEffect?.Alias
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
    
    private async Task FlushOrAddToPending(EffectId effectId, StoredEffect? storedEffect, bool flush, bool delete, bool clearChildren)
    {
        AddToPending(effectId, storedEffect, delete, clearChildren);

        if (flush)
            await Flush();   
    }
    
    private readonly SemaphoreSlim _flushSync = new(initialCount: 1, maxCount: 1);
    public async Task Flush()
    {
        await _flushSync.WaitAsync();

        try
        {
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
            
            await _effectsStore.SetEffectResults(_storedId, changes, _storageSession);
            
            lock (_sync)
                foreach (var pendingChange in pendingChanges)
                {
                    if (pendingChange.Operation == CrudOperation.Delete)
                        _effectResults.Remove(pendingChange.Id);
                    else
                        _effectResults[pendingChange.Id] = _effectResults[pendingChange.Id] with
                        {
                            Existing = true,
                            Operation = null
                        };
                }
        }
        
        finally
        {
            _flushSync.Release();
        }
        
        await (QueueManager?.AfterFlush() ?? Task.CompletedTask);
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