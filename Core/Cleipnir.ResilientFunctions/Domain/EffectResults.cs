using System;
using System.Collections.Generic;
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
    private readonly Dictionary<EffectId, StoredEffect> _effectResults = new();
    private readonly Lock _sync = new();
    private readonly Lazy<Task<IReadOnlyList<StoredEffect>>> _lazyExistingEffects;
    private readonly IEffectsStore _effectsStore;
    private readonly ISerializer _serializer;
    private volatile bool _initialized;

    private readonly Dictionary<EffectId, PendingChange> _pendingChanges = new();
        
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
                _effectResults[existingEffect.EffectId] = existingEffect;
            
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
            return _effectResults.GetValueOrDefault(effectId);
    }

    public async Task Set(StoredEffect storedEffect, bool flush)
    {
        await InitializeIfRequired();

        await _effectsStore.SetEffectResult(_storedId, storedEffect);
        lock (_sync)
            _effectResults[storedEffect.EffectId] = storedEffect;
    }
    
    public async Task<T> CreateOrGet<T>(EffectId effectId, T value, bool flush)
    {
        await InitializeIfRequired();
        
        lock (_sync)
        {
            if (_effectResults.TryGetValue(effectId, out var existing) && existing.WorkStatus == WorkStatus.Completed)
                return _serializer.Deserialize<T>(existing.Result!);
            
            if (existing?.StoredException != null)
                throw _serializer.DeserializeException(_flowId, existing.StoredException!);
        }

        var storedEffect = StoredEffect.CreateCompleted(effectId, _serializer.Serialize(value));
        await _effectsStore.SetEffectResult(_storedId, storedEffect);

        lock (_sync)
            _effectResults[effectId] = storedEffect;
        
        return value;
    }
    
    internal async Task Upsert<T>(EffectId effectId, T value, bool flush)
    {
        await InitializeIfRequired();
        
        var storedEffect = StoredEffect.CreateCompleted(effectId, _serializer.Serialize(value));
        await _effectsStore.SetEffectResult(_storedId, storedEffect);
        
        lock (_sync)
            _effectResults[effectId] = storedEffect;
    }
    
    public async Task<Option<T>> TryGet<T>(EffectId effectId)
    {
        await InitializeIfRequired();
        
        lock (_sync)
        {
            if (_effectResults.TryGetValue(effectId, out var storedEffect))
            {
                if (storedEffect.WorkStatus == WorkStatus.Completed)
                {
                    var value = _serializer.Deserialize<T>(storedEffect.Result!)!;
                    return Option.Create(value);    
                }
                
                if (storedEffect.StoredException != null)
                    throw _serializer.DeserializeException(_flowId, storedEffect.StoredException!);
            }
        }
        
        return Option<T>.NoValue;
    }
    
    public async Task InnerCapture(string id, EffectType effectType, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext, bool flush)
    {
        await InitializeIfRequired();
        
        var effectId = id.ToEffectId(effectType, context: effectContext.Parent?.Serialize());
        EffectContext.SetParent(effectId);
        
        lock (_sync)
        {
            var success = _effectResults.TryGetValue(effectId, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw _serializer.DeserializeException(_flowId, storedEffect.StoredException!);
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            var storedEffect = StoredEffect.CreateStarted(effectId); 
            await _effectsStore.SetEffectResult(_storedId, storedEffect);
            lock (_sync)
                _effectResults[effectId] = storedEffect;
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
            await _effectsStore.SetEffectResult(_storedId, storedEffect);
            
            lock (_sync)
                _effectResults[effectId] = storedEffect;

            exception.FlowId = _flowId;
            throw;
        }
        catch (Exception exception)
        {
            var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(_flowId, exception);
            var storedException = _serializer.SerializeException(fatalWorkflowException);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException);
            await _effectsStore.SetEffectResult(_storedId, storedEffect);
            
            lock (_sync)
                _effectResults[effectId] = storedEffect;

            throw fatalWorkflowException;
        }

        var effectResult = StoredEffect.CreateCompleted(effectId);
        await _effectsStore.SetEffectResult(_storedId, effectResult);

        lock (_sync)
            _effectResults[effectId] = effectResult;
    }
    
    public async Task<T> InnerCapture<T>(string id, EffectType effectType, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext, bool flush)
    {
        await InitializeIfRequired();
        
        var effectId = id.ToEffectId(effectType, context: effectContext.Parent?.Serialize());
        EffectContext.SetParent(effectId);
        
        lock (_sync)
        {
            var success = _effectResults.TryGetValue(effectId, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return (storedEffect.Result == null ? default : _serializer.Deserialize<T>(storedEffect.Result))!;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw FatalWorkflowException.Create(_flowId, storedEffect.StoredException!);
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            var storedEffect = StoredEffect.CreateStarted(effectId);
            await _effectsStore.SetEffectResult(_storedId, storedEffect);
            lock (_sync)
                _effectResults[effectId] = storedEffect;
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
            await _effectsStore.SetEffectResult(_storedId, storedEffect);

            lock (_sync)
                _effectResults[effectId] = storedEffect;

            exception.FlowId = _flowId;
            throw;
        }
        catch (Exception exception)
        {
            var fatalWorkflowException = FatalWorkflowException.CreateNonGeneric(_flowId, exception);
            var storedException = _serializer.SerializeException(fatalWorkflowException);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException);
            await _effectsStore.SetEffectResult(_storedId, storedEffect);

            lock (_sync)
                _effectResults[effectId] = storedEffect;

            throw fatalWorkflowException;
        }

        var effectResult = StoredEffect.CreateCompleted(effectId, _serializer.Serialize(result)); 
        await _effectsStore.SetEffectResult(_storedId, effectResult);

        lock (_sync)
            _effectResults[effectId] = effectResult;
        
        return result;
    }
    
    public async Task Clear(EffectId effectId, bool flush)
    {
        await InitializeIfRequired();
        
        lock (_sync)
            if (!_effectResults.ContainsKey(effectId))
                return;
        
        await _effectsStore.DeleteEffectResult(_storedId, effectId.ToStoredEffectId());
        lock (_sync)
            _effectResults.Remove(effectId);
    }
}