using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public enum ResiliencyLevel
{
    AtLeastOnce,
    AtMostOnce
}

public class Effect
{
    private readonly Func<Task<IEnumerable<StoredEffect>>> _existingEffectsFunc;
    private Dictionary<EffectId, StoredEffect>? _effectResults;

    private readonly IEffectsStore _effectsStore;
    private readonly ISerializer _serializer;
    private readonly FlowId _flowId;
    private readonly object _sync = new();
    
    public Effect(FlowId flowId, Func<Task<IEnumerable<StoredEffect>>> existingEffectsFunc, IEffectsStore effectsStore, ISerializer serializer)
    {
        _flowId = flowId;
        _existingEffectsFunc = existingEffectsFunc;
        _effectsStore = effectsStore;
        _serializer = serializer;
    }
    
    private async Task<Dictionary<EffectId, StoredEffect>> GetEffectResults()
    {
        lock (_sync)
            if (_effectResults is not null)
                return _effectResults;

        var existingEffects = await _existingEffectsFunc();
        var effectResults = existingEffects
            .ToDictionary(e => e.EffectId, e => e); 
        
        lock (_sync)
            if (_effectResults is null)
                _effectResults = effectResults;
            else
                effectResults = _effectResults;
        
        return effectResults;
    }
    
    public async Task<bool> Contains(string id)
    {
        var effectResults = await GetEffectResults();
        
        lock (_sync)
            return effectResults.ContainsKey(id);
    }

    public async Task<T> CreateOrGet<T>(string id, T value)
    {
        var effectResults = await GetEffectResults();
        lock (_sync)
        {
            if (effectResults.TryGetValue(id, out var existing) && existing.WorkStatus == WorkStatus.Completed)
                return _serializer.DeserializeEffectResult<T>(existing.Result!);
            
            if (existing?.StoredException != null)
                throw new EffectException(_flowId, id, _serializer.DeserializeException(existing.StoredException!));
        }
        
        var storedEffect = new StoredEffect(id, WorkStatus.Completed, _serializer.SerializeEffectResult(value), StoredException: null);
        await _effectsStore.SetEffectResult(_flowId, storedEffect);

        lock (_sync)
            effectResults[id] = storedEffect;
        
        return value;
    }

    public async Task Upsert<T>(string id, T value)
    {
        var effectResults = await GetEffectResults();
        
        var storedEffect = new StoredEffect(id, WorkStatus.Completed, _serializer.SerializeEffectResult(value), StoredException: null);
        await _effectsStore.SetEffectResult(_flowId, storedEffect);
        
        lock (_sync)
            effectResults[id] = storedEffect;
    }

    public async Task<Option<T>> TryGet<T>(string id)
    {
        var effectResults = await GetEffectResults();
        
        lock (_sync)
        {
            if (effectResults.TryGetValue(id, out var storedEffect))
            {
                if (storedEffect.WorkStatus == WorkStatus.Completed)
                {
                    var value = _serializer.DeserializeEffectResult<T>(storedEffect.Result!)!;
                    return new Option<T>(value);    
                }
                
                if (storedEffect.StoredException != null)
                    throw new EffectException(_flowId, id, _serializer.DeserializeException(storedEffect.StoredException!));
            }
        }
        
        return Option<T>.NoValue;
    }
    
    public async Task<T> Get<T>(string id)
    {
        var option = await TryGet<T>(id);
        
        if (!option.HasValue)
            throw new InvalidOperationException($"No value exists for id: '{id}'");

        return option.Value;
    }
    
    public Task Capture(string id, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => { work(); return Task.CompletedTask; }, resiliency);
    public Task<T> Capture<T>(string id, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => work().ToTask(), resiliency);

    public async Task Capture(string id, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
    {
        var effectResults = await GetEffectResults();
        lock (_sync)
        {
            var success = effectResults.TryGetValue(id, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw new EffectException(_flowId, id, _serializer.DeserializeException(storedEffect.StoredException!));
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            await _effectsStore.SetEffectResult(
                _flowId,
                new StoredEffect(id, WorkStatus.Started, Result: null, StoredException: null)
            );
            lock (_sync)
                effectResults[id] = new StoredEffect(id, WorkStatus.Started, Result: null, StoredException: null);
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
        catch (Exception exception)
        {
            var storedException = _serializer.SerializeException(exception);
            var storedEffect = new StoredEffect(id, WorkStatus.Failed, Result: null, storedException);
            await _effectsStore.SetEffectResult(_flowId, storedEffect);
            
            lock (_sync)
                effectResults[id] = new StoredEffect(id, WorkStatus.Failed, Result: null, StoredException: storedException);

            throw;
        }

        var effectResult = new StoredEffect(id, WorkStatus.Completed, Result: null, StoredException: null);
        await _effectsStore.SetEffectResult(_flowId,effectResult);

        lock (_sync)
            effectResults[id] = effectResult;
    }
    
    public async Task<T> Capture<T>(string id, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
    {
        var effectResults = await GetEffectResults();
        lock (_sync)
        {
            var success = effectResults.TryGetValue(id, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return (storedEffect.Result == null ? default : JsonSerializer.Deserialize<T>(storedEffect.Result))!;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw new PreviousInvocationException(_flowId, _serializer.DeserializeException(storedEffect.StoredException!));
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            await _effectsStore.SetEffectResult(
                _flowId,
                new StoredEffect(id, WorkStatus.Started, Result: null, StoredException: null)
            );
            lock (_sync)
                effectResults[id] = new StoredEffect(id, WorkStatus.Started, Result: null, StoredException: null);
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
        catch (Exception exception)
        {
            var storedException = _serializer.SerializeException(exception);
            var storedEffect = new StoredEffect(id, WorkStatus.Failed, Result: null, storedException);
            await _effectsStore.SetEffectResult(_flowId, storedEffect);
            
            lock (_sync)
                effectResults[id] = new StoredEffect(id, WorkStatus.Failed, Result: null, StoredException: storedException);

            throw;
        }

        var effectResult = new StoredEffect(id, WorkStatus.Completed, Result: _serializer.SerializeEffectResult(result), StoredException: null);
        await _effectsStore.SetEffectResult(_flowId,effectResult);

        lock (_sync)
            effectResults[id] = effectResult;
        
        return result;
    }

    public async Task Clear(string id)
    {
        var effectResults = await GetEffectResults();
        lock (_sync)
            if (!effectResults.ContainsKey(id))
                return;
        
        await _effectsStore.DeleteEffectResult(_flowId, id);
        lock (_sync)
            effectResults.Remove(id);
    }
    
    public Task<T> WhenAny<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: async () => await await Task.WhenAny(tasks));
    
    public Task<T[]> WhenAll<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: () => Task.WhenAll(tasks));
}