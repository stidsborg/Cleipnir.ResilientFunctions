using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public enum ResiliencyLevel
{
    AtLeastOnce,
    AtMostOnce
}

public class Effect(
    FlowId flowId,
    StoredId storedId,
    Lazy<Task<IReadOnlyList<StoredEffect>>> lazyExistingEffects,
    IEffectsStore effectsStore,
    ISerializer serializer
    )
{
    private Dictionary<EffectId, StoredEffect>? _effectResults;
    private readonly Lock _sync = new();

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

    public async Task<bool> Contains(string id) => await Contains(CreateEffectId(id, EffectType.Effect));
    internal async Task<bool> Contains(EffectId effectId)
    {
        var effectResults = await GetEffectResults();
        
        lock (_sync)
            return effectResults.ContainsKey(effectId);
    }

    public async Task<WorkStatus?> GetStatus(string id)
    {
        var effectId = CreateEffectId(id);
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
        var effectId = CreateEffectId(id);
        lock (_sync)
            if (effectResults.ContainsKey(effectId))
                return false;
        
        var storedEffect = StoredEffect.CreateCompleted(effectId);
        await effectsStore.SetEffectResult(storedId, storedEffect);
        effectResults[effectId] = storedEffect;

        return true;
    }

    public Task<T> CreateOrGet<T>(string id, T value) => CreateOrGet(CreateEffectId(id), value);
    internal async Task<T> CreateOrGet<T>(EffectId effectId, T value)
    {
        var effectResults = await GetEffectResults();
        lock (_sync)
        {
            if (effectResults.TryGetValue(effectId, out var existing) && existing.WorkStatus == WorkStatus.Completed)
                return serializer.DeserializeEffectResult<T>(existing.Result!);
            
            if (existing?.StoredException != null)
                throw serializer.DeserializeException(flowId, existing.StoredException!);
        }

        var storedEffect = StoredEffect.CreateCompleted(effectId, serializer.SerializeEffectResult(value));
        await effectsStore.SetEffectResult(storedId, storedEffect);

        lock (_sync)
            effectResults[effectId] = storedEffect;
        
        return value;
    }

    public async Task Upsert<T>(string id, T value) => await Upsert(CreateEffectId(id, EffectType.Effect), value);
    internal async Task Upsert<T>(EffectId effectId, T value)
    {
        var effectResults = await GetEffectResults();
        
        var storedEffect = StoredEffect.CreateCompleted(effectId, serializer.SerializeEffectResult(value));
        await effectsStore.SetEffectResult(storedId, storedEffect);
        
        lock (_sync)
            effectResults[effectId] = storedEffect;
    }

    public async Task<Option<T>> TryGet<T>(string id) => await TryGet<T>(CreateEffectId(id, EffectType.Effect));
    internal async Task<Option<T>> TryGet<T>(EffectId effectId)
    {
        var effectResults = await GetEffectResults();
        
        lock (_sync)
        {
            if (effectResults.TryGetValue(effectId, out var storedEffect))
            {
                if (storedEffect.WorkStatus == WorkStatus.Completed)
                {
                    var value = serializer.DeserializeEffectResult<T>(storedEffect.Result!)!;
                    return Option.Create(value);    
                }
                
                if (storedEffect.StoredException != null)
                    throw serializer.DeserializeException(flowId, storedEffect.StoredException!);
            }
        }
        
        return Option<T>.NoValue;
    }

    public async Task<T> Get<T>(string id) => await Get<T>(CreateEffectId(id, EffectType.Effect));
    internal async Task<T> Get<T>(EffectId effectId)
    {
        var option = await TryGet<T>(effectId);
        
        if (!option.HasValue)
            throw new InvalidOperationException($"No value exists for id: '{effectId}'");

        return option.Value;
    }

    #region Implicit ids

    public Task Capture(Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId(), work, resiliency);
    public Task<T> Capture<T>(Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId(), work: () => work().ToTask(), resiliency);
    public Task Capture(Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId(), work, resiliency);
    public Task<T> Capture<T>(Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId(), work, resiliency);
    
    #endregion
    
    public Task Capture(string id, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => { work(); return Task.CompletedTask; }, resiliency);
    public Task<T> Capture<T>(string id, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => work().ToTask(), resiliency);
    public async Task Capture(string id, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce) 
        => await InnerCapture(id, EffectType.Effect, work, resiliency, EffectContext.CurrentContext);
    public async Task<T> Capture<T>(string id, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce) 
        => await InnerCapture(id, EffectType.Effect, work, resiliency, EffectContext.CurrentContext);
    
    private async Task InnerCapture(string id, EffectType effectType, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext)
    {
        var effectResults = await GetEffectResults();
        
        var effectId = id.ToEffectId(effectType, context: effectContext.Parent?.Serialize());
        EffectContext.SetParent(effectId);
        
        lock (_sync)
        {
            var success = effectResults.TryGetValue(effectId, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw serializer.DeserializeException(flowId, storedEffect.StoredException!);
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
            var storedException = serializer.SerializeException(FatalWorkflowException.CreateNonGeneric(flowId, exception));
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
    
    private async Task<T> InnerCapture<T>(string id, EffectType effectType, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext)
    {
        var effectResults = await GetEffectResults();
        
        var effectId = id.ToEffectId(effectType, context: effectContext.Parent?.Serialize());
        EffectContext.SetParent(effectId);
        
        lock (_sync)
        {
            var success = effectResults.TryGetValue(effectId, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return (storedEffect.Result == null ? default : JsonSerializer.Deserialize<T>(storedEffect.Result))!;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw FatalWorkflowException.Create(flowId, storedEffect.StoredException!);
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
            var storedException = serializer.SerializeException(FatalWorkflowException.CreateNonGeneric(flowId, exception));
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
        var effectId = CreateEffectId(id);
        lock (_sync)
            if (!effectResults.ContainsKey(effectId))
                return;
        
        await effectsStore.DeleteEffectResult(storedId, effectId.ToStoredEffectId());
        lock (_sync)
            effectResults.Remove(effectId);
    }
    
    public Task<T> WhenAny<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: async () => await await Task.WhenAny(tasks));
    public Task<T[]> WhenAll<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: () => Task.WhenAll(tasks));

    public Task<T> WhenAny<T>(params Task<T>[] tasks)
        => WhenAny(EffectContext.CurrentContext.NextImplicitId(), tasks);
    public Task<T[]> WhenAll<T>(params Task<T>[] tasks)
        => WhenAll(EffectContext.CurrentContext.NextImplicitId(), tasks);

    internal string TakeNextImplicitId() => EffectContext.CurrentContext.NextImplicitId();

    internal EffectId CreateEffectId(string id, EffectType? type = null) 
        => id.ToEffectId(type, context: EffectContext.CurrentContext.Parent?.Serialize());
}