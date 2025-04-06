using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Serialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public class EffectResults
{
    private readonly StoredId _storedId;
    private readonly FlowId _flowId;
    private readonly Lock _sync = new();
    private readonly Lazy<Task<IReadOnlyList<StoredEffect>>> _lazyExistingEffects;
    private readonly IEffectsStore _effectsStore;
    private readonly ISerializer _serializer;
    private volatile bool _initialized;

    public Dictionary<EffectId, PendingEffectChange> PendingChanges { get; } = new();

    private volatile bool _hasPendingChanges;
    public bool HasPendingChanges
    {
        get => _hasPendingChanges;
        private set => _hasPendingChanges = value;
    }
        
    public EffectResults(
        FlowId flowId,
        StoredId storedId,
        Lazy<Task<IReadOnlyList<StoredEffect>>> lazyExistingEffects,
        IEffectsStore effectsStore,
        ISerializer serializer)
    {
        _flowId = flowId;
        _storedId = storedId;
        _lazyExistingEffects = lazyExistingEffects;
        _effectsStore = effectsStore;
        _serializer = serializer;
    }

    private async Task InitializeIfRequired()
    {
        if (_initialized)
            return;
        
        var existingEffects = await _lazyExistingEffects.Value;
        lock (_sync)
        {
            if (_initialized)
                return;

            foreach (var existingEffect in existingEffects)
                PendingChanges[existingEffect.EffectId] =
                    new PendingEffectChange(
                        existingEffect.StoredEffectId,
                        existingEffect,
                        Operation: null,
                        Existing: true
                    );
            
            _initialized = true;
        }
    }

    public async Task<bool> Contains(EffectId effectId)
    {
        await InitializeIfRequired();
        lock (_sync)
            return PendingChanges.ContainsKey(effectId);
    }

    public async Task<StoredEffect?> GetOrValueDefault(EffectId effectId)
    {
        await InitializeIfRequired();
        lock (_sync)
            return PendingChanges.GetValueOrDefault(effectId)?.StoredEffect;
    }

    public async Task Set(StoredEffect storedEffect, bool flush)
    {
        await InitializeIfRequired();
        await FlushOrAddToPending(
            storedEffect.EffectId,
            storedEffect.StoredEffectId,
            storedEffect,
            flush,
            delete: false
        );
    }

    public async Task<T> CreateOrGet<T>(EffectId effectId, T value, bool flush)
    {
        await InitializeIfRequired();
        lock (_sync)
        {
            if (PendingChanges.TryGetValue(effectId, out var existing) && existing.StoredEffect?.WorkStatus == WorkStatus.Completed)
                return _serializer.Deserialize<T>(existing.StoredEffect.Result!);
            
            if (existing?.StoredEffect?.StoredException != null)
                throw _serializer.DeserializeException(_flowId, existing.StoredEffect.StoredException!);
        }

        var storedEffect = StoredEffect.CreateCompleted(effectId, _serializer.Serialize(value));
        await FlushOrAddToPending(
            storedEffect.EffectId,
            storedEffect.StoredEffectId,
            storedEffect,
            flush,
            delete: false
        );
        
        return value;
    }
    
    internal async Task Upsert<T>(EffectId effectId, T value, bool flush)
    {
        await InitializeIfRequired();
        
        var storedEffect = StoredEffect.CreateCompleted(effectId, _serializer.Serialize(value));
        await FlushOrAddToPending(
            storedEffect.EffectId,
            storedEffect.StoredEffectId,
            storedEffect,
            flush,
            delete: false
        );
    }
    
    public async Task<Option<T>> TryGet<T>(EffectId effectId)
    {
        await InitializeIfRequired();
        
        lock (_sync)
        {
            if (PendingChanges.TryGetValue(effectId, out var change))
            {
                var storedEffect = change.StoredEffect;
                if (storedEffect?.WorkStatus == WorkStatus.Completed)
                {
                    var value = _serializer.Deserialize<T>(storedEffect.Result!)!;
                    return Option.Create(value);    
                }
                
                if (storedEffect?.StoredException != null)
                    throw _serializer.DeserializeException(_flowId, storedEffect.StoredException!);
            }
        }
        
        return Option<T>.NoValue;
    }
    
    public async Task InnerCapture(string id, EffectType effectType, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext)
    {
        await InitializeIfRequired();
        
        var effectId = id.ToEffectId(effectType, context: effectContext.Parent?.Serialize());
        EffectContext.SetParent(effectId);
        
        lock (_sync)
        {
            var success = PendingChanges.TryGetValue(effectId, out var pendingChange);
            var storedEffect = pendingChange?.StoredEffect;
            if (success && storedEffect?.WorkStatus == WorkStatus.Completed)
                return;
            if (success && storedEffect?.WorkStatus == WorkStatus.Failed)
                throw _serializer.DeserializeException(_flowId, storedEffect.StoredException!);
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            var storedEffect = StoredEffect.CreateStarted(effectId);
            await FlushOrAddToPending(effectId, storedEffect.StoredEffectId, storedEffect, flush: true, delete: false);
        }

        try
        {
            await work();
        }
        catch (PostponeInvocationException)
        {
            throw;
        }
        catch (SuspendInvocationException)
        {
            throw;
        }
        catch (FatalWorkflowException exception)
        {
            var storedException = _serializer.SerializeException(exception);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect.StoredEffectId,
                storedEffect,
                flush: true,
                delete: false
            );

            exception.FlowId = _flowId;
            throw;
        }
        catch (Exception exception)
        {
            var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(_flowId, exception);
            var storedException = _serializer.SerializeException(fatalWorkflowException);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect.StoredEffectId,
                storedEffect,
                flush: true,
                delete: false
            );

            throw fatalWorkflowException;
        }

        {
            var storedEffect = StoredEffect.CreateCompleted(effectId);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect.StoredEffectId,
                storedEffect,
                flush: resiliency != ResiliencyLevel.AtLeastOnceDelayFlush,
                delete: false
            );    
        }
    }
    
    public async Task<T> InnerCapture<T>(string id, EffectType effectType, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext)
    {
        await InitializeIfRequired();
        
        var effectId = id.ToEffectId(effectType, context: effectContext.Parent?.Serialize());
        EffectContext.SetParent(effectId);
        
        lock (_sync)
        {
            var success = PendingChanges.TryGetValue(effectId, out var storedEffect);
            if (success && storedEffect!.StoredEffect?.WorkStatus == WorkStatus.Completed)
                return (storedEffect.StoredEffect?.Result == null ? default : _serializer.Deserialize<T>(storedEffect.StoredEffect?.Result!))!;
            if (success && storedEffect!.StoredEffect?.WorkStatus == WorkStatus.Failed)
                throw FatalWorkflowException.Create(_flowId, storedEffect.StoredEffect?.StoredException!);
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            var storedEffect = StoredEffect.CreateStarted(effectId);
            await FlushOrAddToPending(
                effectId,
                storedEffect.StoredEffectId,
                storedEffect,
                flush: true,
                delete: false
            );
        }

        T result;
        try
        {
            result = await work();
        }
        catch (PostponeInvocationException)
        {
            throw;
        }
        catch (SuspendInvocationException)
        {
            throw;
        }
        catch (FatalWorkflowException exception)
        {
            var storedException = _serializer.SerializeException(exception);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException);
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect.StoredEffectId,
                storedEffect,
                flush: true,
                delete: false
            );

            exception.FlowId = _flowId;
            throw;
        }
        catch (Exception exception)
        {
            var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(_flowId, exception);
            var storedException = _serializer.SerializeException(fatalWorkflowException);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException);

            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect.StoredEffectId,
                storedEffect,
                flush: true,
                delete: false
            );
            throw fatalWorkflowException;
        }

        {
            var storedEffect = StoredEffect.CreateCompleted(effectId, _serializer.Serialize(result)); 
            await FlushOrAddToPending(
                storedEffect.EffectId,
                storedEffect.StoredEffectId,
                storedEffect,
                flush: resiliency != ResiliencyLevel.AtLeastOnceDelayFlush,
                delete: false
            );
        
            return result;   
        }
    }
    
    public async Task Clear(EffectId effectId, bool flush)
    {
        await InitializeIfRequired();
        
        lock (_sync)
            if (!PendingChanges.ContainsKey(effectId))
                return;
        
        await FlushOrAddToPending(
            effectId,
            effectId.ToStoredEffectId(),
            storedEffect: null,
            flush,
            delete: true
        );
    }

    private async Task FlushOrAddToPending(EffectId effectId, StoredEffectId storedEffectId, StoredEffect? storedEffect, bool flush, bool delete)
    {
        lock (_sync)
            if (PendingChanges.ContainsKey(effectId))
            {
                var existing = PendingChanges[effectId];
                PendingChanges[effectId] = existing with
                {
                    StoredEffect = storedEffect,
                    Operation = delete 
                        ? CrudOperation.Delete
                        : (existing.Existing ? CrudOperation.Update : CrudOperation.Insert)
                };
            }
            else
            {
                PendingChanges[effectId] = new PendingEffectChange(
                    storedEffectId,
                    storedEffect,
                    CrudOperation.Insert,
                    Existing: false
                );
            }

        if (flush) 
            await Flush();

        HasPendingChanges = !flush;
    }
    
    private readonly SemaphoreSlim _flushSync = new(initialCount: 1, maxCount: 1);
    public async Task Flush()
    {
        await _flushSync.WaitAsync();
        try
        {
            IReadOnlyList<PendingEffectChange> pendingChanges;
            lock (_sync)
                pendingChanges = PendingChanges.Values.Where(r => r.Operation != null).ToList();
            
            if (pendingChanges.Count == 0)
                return;

            var changes = pendingChanges
                .Select(pc =>
                    new StoredEffectChange(
                        _storedId,
                        pc.Id,
                        pc.Operation!.Value,
                        pc.StoredEffect
                    )
                ).ToList();
            
            await _effectsStore.SetEffectResults(_storedId, changes);
            
            lock (_sync)
                foreach (var (key, value) in PendingChanges.ToList())
                {
                    if (value.Operation == CrudOperation.Delete)
                        PendingChanges.Remove(key);
                    else
                        PendingChanges[key] = value with
                        {
                            Existing = true,
                            Operation = null
                        };
                }

            HasPendingChanges = false;
        }
        finally
        {
            _flushSync.Release();
        }
    }
}