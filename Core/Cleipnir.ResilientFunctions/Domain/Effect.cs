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

public class Effect(
    FlowType flowType,
    StoredId storedId,
    Lazy<Task<IReadOnlyList<StoredEffect>>> lazyExistingEffects,
    IEffectsStore effectsStore,
    ISerializer serializer
    )
{
    private readonly ImplicitIds _implicitIds = new();
    private Dictionary<EffectId, StoredEffect>? _effectResults;
    private readonly object _sync = new();

    private async Task<Dictionary<EffectId, StoredEffect>> GetEffectResults()
    {
        lock (_sync)
            if (_effectResults is not null)
                return _effectResults;

        var existingEffects = await lazyExistingEffects.Value;
        var effectResults = existingEffects
            .ToDictionary(e => e.EffectId, e => e); 
        
        lock (_sync)
            if (_effectResults is null)
                _effectResults = effectResults;
            else
                effectResults = _effectResults;
        
        return effectResults;
    }

    internal Task<IEnumerable<EffectId>> EffectIds => GetEffectResults()
        .SelectAsync(kv => kv.Keys.AsEnumerable());
    
    public async Task<bool> Contains(string id)
    {
        var effectResults = await GetEffectResults();
        
        lock (_sync)
            return effectResults.ContainsKey(id.ToEffectId());
    }

    public async Task<WorkStatus?> GetStatus(string id)
    {
        var effectId = id.ToEffectId();
        var effectResults = await GetEffectResults();

        lock (_sync)
            if (effectResults.TryGetValue(effectId, out var value))
                return value.WorkStatus;
            else
                return null;
    }
    
    public async Task<bool> Mark(string id)
    {
        var effectResults = await GetEffectResults();
        var effectId = id.ToEffectId();
        lock (_sync)
            if (effectResults.ContainsKey(id.ToEffectId()))
                return false;
        
        var storedEffect = StoredEffect.CreateCompleted(effectId);
        await effectsStore.SetEffectResult(storedId, storedEffect);
        effectResults[effectId] = storedEffect;

        return true;
    }

    public async Task<T> CreateOrGet<T>(string id, T value)
    {
        var effectResults = await GetEffectResults();
        var effectId = id.ToEffectId();
        lock (_sync)
        {
            if (effectResults.TryGetValue(effectId, out var existing) && existing.WorkStatus == WorkStatus.Completed)
                return serializer.DeserializeEffectResult<T>(existing.Result!);
            
            if (existing?.StoredException != null)
                throw new EffectException(flowType, id, serializer.DeserializeException(existing.StoredException!));
        }

        var storedEffect = StoredEffect.CreateCompleted(effectId, serializer.SerializeEffectResult(value));
        await effectsStore.SetEffectResult(storedId, storedEffect);

        lock (_sync)
            effectResults[effectId] = storedEffect;
        
        return value;
    }

    public async Task Upsert<T>(string id, T value)
    {
        var effectResults = await GetEffectResults();
        var effectId = id.ToEffectId();
        
        var storedEffect = StoredEffect.CreateCompleted(effectId, serializer.SerializeEffectResult(value));
        await effectsStore.SetEffectResult(storedId, storedEffect);
        
        lock (_sync)
            effectResults[effectId] = storedEffect;
    }

    public async Task<Option<T>> TryGet<T>(string id)
    {
        var effectResults = await GetEffectResults();
        var effectId = id.ToEffectId();
        
        lock (_sync)
        {
            if (effectResults.TryGetValue(effectId, out var storedEffect))
            {
                if (storedEffect.WorkStatus == WorkStatus.Completed)
                {
                    var value = serializer.DeserializeEffectResult<T>(storedEffect.Result!)!;
                    return new Option<T>(value);    
                }
                
                if (storedEffect.StoredException != null)
                    throw new EffectException(flowType, id, serializer.DeserializeException(storedEffect.StoredException!));
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

    #region Implicit ids

    public Task Capture(Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: _implicitIds.Next(), work, resiliency);
    public Task<T> Capture<T>(Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: _implicitIds.Next(), work: () => work().ToTask(), resiliency);
    public Task Capture(Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: _implicitIds.Next(), work, resiliency);
    public Task<T> Capture<T>(Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: _implicitIds.Next(), work, resiliency);
    
    #endregion
    
    public Task Capture(string id, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => { work(); return Task.CompletedTask; }, resiliency);
    public Task<T> Capture<T>(string id, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => work().ToTask(), resiliency);
    public async Task Capture(string id, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
    {
        var effectResults = await GetEffectResults();
        var effectId = id.ToEffectId();
        lock (_sync)
        {
            var success = effectResults.TryGetValue(effectId, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw new EffectException(flowType, id, serializer.DeserializeException(storedEffect.StoredException!));
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            var storedEffect = StoredEffect.CreateStarted(effectId); 
            await effectsStore.SetEffectResult(storedId, storedEffect);
            lock (_sync)
                effectResults[effectId] = storedEffect;
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
            var storedException = serializer.SerializeException(exception);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException);
            await effectsStore.SetEffectResult(storedId, storedEffect);
            
            lock (_sync)
                effectResults[effectId] = storedEffect;

            throw;
        }

        var effectResult = StoredEffect.CreateCompleted(effectId);
        await effectsStore.SetEffectResult(storedId,effectResult);

        lock (_sync)
            effectResults[effectId] = effectResult;
    }
    
    public async Task<T> Capture<T>(string id, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
    {
        var effectResults = await GetEffectResults();
        var effectId = id.ToEffectId();
        lock (_sync)
        {
            var success = effectResults.TryGetValue(effectId, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return (storedEffect.Result == null ? default : JsonSerializer.Deserialize<T>(storedEffect.Result))!;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw new PreviousInvocationException(flowType, serializer.DeserializeException(storedEffect.StoredException!));
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            var storedEffect = StoredEffect.CreateStarted(effectId);
            await effectsStore.SetEffectResult(storedId, storedEffect);
            lock (_sync)
                effectResults[effectId] = storedEffect;
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
            var storedException = serializer.SerializeException(exception);
            var storedEffect = StoredEffect.CreateFailed(effectId, storedException);
            await effectsStore.SetEffectResult(storedId, storedEffect);

            lock (_sync)
                effectResults[effectId] = storedEffect;

            throw;
        }

        var effectResult = StoredEffect.CreateCompleted(effectId, serializer.SerializeEffectResult(result)); 
        await effectsStore.SetEffectResult(storedId, effectResult);

        lock (_sync)
            effectResults[effectId] = effectResult;
        
        return result;
    }

    public async Task Clear(string id)
    {
        var effectResults = await GetEffectResults();
        var effectId = id.ToEffectId();
        lock (_sync)
            if (!effectResults.ContainsKey(effectId))
                return;
        
        await effectsStore.DeleteEffectResult(storedId, id.ToStoredEffectId(isState: false));
        lock (_sync)
            effectResults.Remove(effectId);
    }
    
    public Task<T> WhenAny<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: async () => await await Task.WhenAny(tasks));
    public Task<T[]> WhenAll<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: () => Task.WhenAll(tasks));

    public Task<T> WhenAny<T>(params Task<T>[] tasks)
        => WhenAny(_implicitIds.Next(), tasks);
    public Task<T[]> WhenAll<T>(params Task<T>[] tasks)
        => WhenAll(_implicitIds.Next(), tasks);

    internal string TakeNextImplicitId() => _implicitIds.Next();
}