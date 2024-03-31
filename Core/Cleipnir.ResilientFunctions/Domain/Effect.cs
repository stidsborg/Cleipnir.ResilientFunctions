using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public enum ResiliencyLevel
{
    AtLeastOnce,
    AtMostOnce
}

public class Effect
{
    private readonly Dictionary<EffectId, StoredEffect> _effectResults;
    private readonly IEffectsStore _effectsStore;
    private readonly ISerializer _serializer;
    private readonly FunctionId _functionId;
    private readonly object _sync = new();
    
    public Effect(FunctionId functionId, IEnumerable<StoredEffect> existingEffects, IEffectsStore effectsStore, ISerializer serializer)
    {
        _functionId = functionId;
        _effectsStore = effectsStore;
        _serializer = serializer;

        _effectResults = existingEffects.ToDictionary(sa => sa.EffectId, sa => sa);
    }

    public bool Contains(string id)
    {
        lock (_sync)
            return _effectResults.ContainsKey(id);
    }

    public async Task<T> CreateOrGet<T>(string id, T value)
    {
        lock (_sync)
        {
            if (_effectResults.TryGetValue(id, out var existing) && existing.WorkStatus == WorkStatus.Completed)
                return _serializer.DeserializeEffectResult<T>(existing.Result!);
            
            if (existing?.StoredException != null)
                throw new EffectException(_functionId, id, _serializer.DeserializeException(existing.StoredException!));
        }
        
        var storedEffect = new StoredEffect(id, WorkStatus.Completed, _serializer.SerializeEffectResult(value), StoredException: null);
        await _effectsStore.SetEffectResult(_functionId, storedEffect);

        lock (_sync)
            _effectResults[id] = storedEffect;
        
        return value;
    }

    public async Task Upsert<T>(string id, T value)
    {
        var storedEffect = new StoredEffect(id, WorkStatus.Completed, _serializer.SerializeEffectResult(value), StoredException: null);
        await _effectsStore.SetEffectResult(_functionId, storedEffect);

        lock (_sync)
            _effectResults[id] = storedEffect;
    }

    public bool TryGet<T>(string id, [NotNullWhen(true)] out T? value)
    {
        lock (_sync)
        {
            if (_effectResults.TryGetValue(id, out var storedEffect))
            {
                if (storedEffect.WorkStatus == WorkStatus.Completed)
                {
                    value = _serializer.DeserializeEffectResult<T>(storedEffect.Result!)!;
                    return true;    
                }
                
                if (storedEffect.StoredException != null)
                    throw new EffectException(_functionId, id, _serializer.DeserializeException(storedEffect.StoredException!));
            }
        }

        value = default;
        return false;
    }
    
    public T Get<T>(string id)
    {
        lock (_sync)
        {
            if (!TryGet(id, out T? value))
                throw new InvalidOperationException($"No value exists for id: '{id}'");

            return value!;
        }
    }
    
    public Task Capture(string id, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => { work(); return Task.CompletedTask; }, resiliency);
    public Task<T> Capture<T>(string id, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => work().ToTask(), resiliency);

    public async Task Capture(string id, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
    {
        lock (_sync)
        {
            var success = _effectResults.TryGetValue(id, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw new EffectException(_functionId, id, _serializer.DeserializeException(storedEffect.StoredException!));
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            await _effectsStore.SetEffectResult(
                _functionId,
                new StoredEffect(id, WorkStatus.Started, Result: null, StoredException: null)
            );
            lock (_sync)
                _effectResults[id] = new StoredEffect(id, WorkStatus.Started, Result: null, StoredException: null);
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
            await _effectsStore.SetEffectResult(_functionId, storedEffect);
            
            lock (_sync)
                _effectResults[id] = new StoredEffect(id, WorkStatus.Failed, Result: null, StoredException: storedException);

            throw;
        }

        await _effectsStore.SetEffectResult(
            _functionId,
            new StoredEffect(id, WorkStatus.Completed, Result: null, StoredException: null)
        );
    }
    
    public async Task<T> Capture<T>(string id, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
    {
        lock (_sync)
        {
            var success = _effectResults.TryGetValue(id, out var storedEffect);
            if (success && storedEffect!.WorkStatus == WorkStatus.Completed)
                return (storedEffect.Result == null ? default : JsonSerializer.Deserialize<T>(storedEffect.Result))!;
            if (success && storedEffect!.WorkStatus == WorkStatus.Failed)
                throw new PreviousFunctionInvocationException(_functionId, _serializer.DeserializeException(storedEffect.StoredException!));
            if (success && resiliency == ResiliencyLevel.AtMostOnce)
                throw new InvalidOperationException($"Effect '{id}' started but did not complete previously");
        }

        if (resiliency == ResiliencyLevel.AtMostOnce)
        {
            await _effectsStore.SetEffectResult(
                _functionId,
                new StoredEffect(id, WorkStatus.Started, Result: null, StoredException: null)
            );
            lock (_sync)
                _effectResults[id] = new StoredEffect(id, WorkStatus.Started, Result: null, StoredException: null);
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
            await _effectsStore.SetEffectResult(_functionId, storedEffect);
            
            lock (_sync)
                _effectResults[id] = new StoredEffect(id, WorkStatus.Failed, Result: null, StoredException: storedException);

            throw;
        }

        await _effectsStore.SetEffectResult(
            _functionId,
            new StoredEffect(id, WorkStatus.Completed, Result: _serializer.SerializeEffectResult(result), StoredException: null)
        );

        return result;
    }

    public async Task Clear(string id)
    {
        lock (_sync)
            if (!_effectResults.ContainsKey(id))
                return;
        
        await _effectsStore.DeleteEffectResult(_functionId, id);
        lock (_sync)
            _effectResults.Remove(id);
    }
    
    public Task<T> WhenAny<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: async () => await await Task.WhenAny(tasks));
    
    public Task<T[]> WhenAll<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: () => Task.WhenAll(tasks));
}