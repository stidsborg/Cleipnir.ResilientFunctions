using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Session;

namespace Cleipnir.ResilientFunctions.Domain;

public class EffectResults(
    FlowId flowId,
    StoredId storedId,
    Lazy<Task<IReadOnlyList<StoredEffect>>> lazyExistingEffects,
    IEffectsStore effectsStore,
    ISerializer serializer,
    IStorageSession? storageSession)
{
    private readonly Lock _sync = new();
    private volatile bool _initialized;

    private readonly Dictionary<EffectId, PendingEffectChange> _effectResults = new();
    
    public async Task<EffectId?> GetEffectId(string alias)
    {
        await InitializeIfRequired();
        lock (_sync)
            return _effectResults
                .Values
                .FirstOrDefault(c => c.Alias == alias)
                ?.Id;
    }

    public IEnumerable<EffectId> EffectIds => _effectResults.Keys.ToList();
    
    private async Task InitializeIfRequired()
    {
        if (_initialized)
            return;
        
        var existingEffects = await lazyExistingEffects.Value;
        lock (_sync)
        {
            if (_initialized)
                return;

            foreach (var existingEffect in existingEffects)
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

    public async Task<bool> Contains(EffectId effectId)
    {
        await InitializeIfRequired();
        lock (_sync)
            return _effectResults.ContainsKey(effectId);
    }

    public async Task<StoredEffect?> GetOrValueDefault(EffectId effectId)
    {
        await InitializeIfRequired();
        lock (_sync)
            return _effectResults.GetValueOrDefault(effectId)?.StoredEffect;
    }

    public async Task Set(StoredEffect storedEffect, bool flush)
    {
        await InitializeIfRequired();
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
        await InitializeIfRequired();
        lock (_sync)
        {
            if (_effectResults.TryGetValue(effectId, out var existing) && existing.StoredEffect?.WorkStatus == WorkStatus.Completed)
                return serializer.Deserialize<T>(existing.StoredEffect.Result!);

            if (existing?.StoredEffect?.StoredException != null)
                throw serializer.DeserializeException(flowId, existing.StoredEffect.StoredException!);
        }

        var storedEffect = StoredEffect.CreateCompleted(effectId, serializer.Serialize(value), alias);
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
        await InitializeIfRequired();
        
        var storedEffect = StoredEffect.CreateCompleted(effectId, serializer.Serialize(value), alias);
        await FlushOrAddToPending(
            storedEffect.EffectId,
            storedEffect,
            flush,
            delete: false,
            clearChildren: false
        );
    }
    
    internal async Task Upserts(IEnumerable<Tuple<EffectId, object, string?>> values, bool flush)
    {
        await InitializeIfRequired();

        var storedEffects = values
            .Select(t => new { Id = t.Item1, Bytes = serializer.Serialize(t.Item2, t.Item2.GetType()), Alias = t.Item3 })
            .Select(a => StoredEffect.CreateCompleted(a.Id, a.Bytes, a.Alias))
            .ToList();

        AddToPending(storedEffects);

        if (flush)
            await Flush();
    }
    
    public async Task<Option<T>> TryGet<T>(EffectId effectId)
    {
        await InitializeIfRequired();
        
        lock (_sync)
        {
            if (_effectResults.TryGetValue(effectId, out var change))
            {
                var storedEffect = change.StoredEffect;
                if (storedEffect?.WorkStatus == WorkStatus.Completed)
                {
                    var value = serializer.Deserialize<T>(storedEffect.Result!)!;
                    return Option.Create(value);    
                }
                
                if (storedEffect?.StoredException != null)
                    throw serializer.DeserializeException(flowId, storedEffect.StoredException!);
            }
        }
        
        return Option<T>.NoValue;
    }
    
    public async Task InnerCapture(EffectId effectId, string? alias, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext)
    {
        await InitializeIfRequired();
        
        EffectContext.SetParent(effectId);

        lock (_sync)
        {
            var success = _effectResults.TryGetValue(effectId, out var pendingChange);
            var storedEffect = pendingChange?.StoredEffect;
            if (success && storedEffect?.WorkStatus == WorkStatus.Completed)
                return;
            if (success && storedEffect?.WorkStatus == WorkStatus.Failed)
                throw serializer.DeserializeException(flowId, storedEffect.StoredException!);
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
            var storedException = serializer.SerializeException(exception);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException, alias);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect,
                flush: true,
                delete: false,
                clearChildren: false
            );

            exception.FlowId = flowId;
            throw;
        }
        catch (Exception exception)
        {
            var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(flowId, exception);
            var storedException = serializer.SerializeException(fatalWorkflowException);
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
                clearChildren: false
            );
        }
    }
    
    public async Task<T> InnerCapture<T>(EffectId effectId, string? alias, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext)
    {
        await InitializeIfRequired();
        
        EffectContext.SetParent(effectId);

        lock (_sync)
        {
            var success = _effectResults.TryGetValue(effectId, out var storedEffect);
            if (success && storedEffect!.StoredEffect?.WorkStatus == WorkStatus.Completed)
                return (storedEffect.StoredEffect?.Result == null ? default : serializer.Deserialize<T>(storedEffect.StoredEffect?.Result!))!;
            if (success && storedEffect!.StoredEffect?.WorkStatus == WorkStatus.Failed)
                throw FatalWorkflowException.Create(flowId, storedEffect.StoredEffect?.StoredException!);
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
            var storedException = serializer.SerializeException(exception);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException, alias);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect,
                flush: true,
                delete: false,
                clearChildren: false
            );

            exception.FlowId = flowId;
            throw;
        }
        catch (Exception exception)
        {
            var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(flowId, exception);
            var storedException = serializer.SerializeException(fatalWorkflowException);
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
            var storedEffect = StoredEffect.CreateCompleted(effectId, serializer.Serialize(result), alias);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect,
                flush: resiliency != ResiliencyLevel.AtLeastOnceDelayFlush,
                delete: false,
                clearChildren: false
            );

            return result;
        }
    }
    
    public async Task Clear(EffectId effectId, bool flush)
    {
        await InitializeIfRequired();

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

    private void AddToPending(IEnumerable<StoredEffect> storedEffects)
    {
        lock (_sync)
            foreach (var storedEffect in storedEffects)
                AddToPending(storedEffect.EffectId, storedEffect, delete: false, clearChildren: false);
    }

    private void AddToPending(EffectId effectId, StoredEffect? storedEffect, bool delete, bool clearChildren)
    {
        lock (_sync)
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

                if (clearChildren)
                {
                    var children = _effectResults.Keys.Where(id => id.IsChild(effectId));
                    foreach (var child in children)
                        _effectResults[child] = 
                            _effectResults[child] with { Operation = CrudOperation.Delete };
                }
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
                        storedId,
                        p.Id,
                        p.Operation!.Value,
                        p.StoredEffect
                    )
                ).ToList();
            
            await effectsStore.SetEffectResults(storedId, changes, storageSession);
            
            lock (_sync)
                foreach (var (key, value) in _effectResults.ToList())
                {
                    if (value.Operation == CrudOperation.Delete)
                        _effectResults.Remove(key);
                    else
                        _effectResults[key] = value with
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
    }

}