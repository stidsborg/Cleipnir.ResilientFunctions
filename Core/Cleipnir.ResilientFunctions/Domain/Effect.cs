using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Reactive.Utilities;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public enum ResiliencyLevel
{
    AtLeastOnce,
    AtMostOnce,
    AtLeastOnceDelayFlush
}

public class Effect(EffectResults effectResults)
{
    public async Task<bool> Contains(string id) => await Contains(CreateEffectId(id, EffectType.Effect));
    internal Task<bool> Contains(EffectId effectId) => effectResults.Contains(effectId);

    public async Task<WorkStatus?> GetStatus(string id)
    {
        var effectId = CreateEffectId(id);
        var storedEffect = await effectResults.GetOrValueDefault(effectId);
        return storedEffect?.WorkStatus;
    }
    
    public async Task<bool> Mark(string id)
    {
        var effectId = CreateEffectId(id);
        if (await effectResults.Contains(effectId))
            return false;
        
        var storedEffect = StoredEffect.CreateCompleted(effectId);
        await effectResults.Set(storedEffect, flush: true);
        return true;
    }

    public Task<T> CreateOrGet<T>(string id, T value) => CreateOrGet(CreateEffectId(id), value);
    internal Task<T> CreateOrGet<T>(EffectId effectId, T value) => effectResults.CreateOrGet(effectId, value, flush: true);

    public async Task Upsert<T>(string id, T value) => await Upsert(CreateEffectId(id, EffectType.Effect), value);
    internal Task Upsert<T>(EffectId effectId, T value) => effectResults.Upsert(effectId, value, flush: true);

    public async Task<Option<T>> TryGet<T>(string id) => await TryGet<T>(CreateEffectId(id, EffectType.Effect));
    private Task<Option<T>> TryGet<T>(EffectId effectId) => effectResults.TryGet<T>(effectId);
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

    private Task InnerCapture(string id, EffectType effectType, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext)
        => effectResults.InnerCapture(id, effectType, work, resiliency, effectContext);
    private Task<T> InnerCapture<T>(string id, EffectType effectType, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext)
        => effectResults.InnerCapture(id, effectType, work, resiliency, effectContext);

    public Task Clear(string id) => effectResults.Clear(CreateEffectId(id), flush: true);
    
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