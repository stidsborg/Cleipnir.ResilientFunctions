using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
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

public class Effect(EffectResults effectResults, UtcNow utcNow, FlowMinimumTimeout flowMinimumTimeout)
{
    public async Task<bool> Contains(string id) => await Contains(CreateEffectId(id, EffectType.Effect));
    internal Task<bool> Contains(EffectId effectId) => effectResults.Contains(effectId);

    internal IEnumerable<EffectId> EffectIds => effectResults.EffectIds;
    internal FlowMinimumTimeout FlowMinimumTimeout => flowMinimumTimeout;

    public async Task<WorkStatus?> GetStatus(string id)
    {
        var effectId = CreateEffectId(id);
        var storedEffect = await effectResults.GetOrValueDefault(effectId);
        return storedEffect?.WorkStatus;
    }
    
    public async Task<bool> Mark(string id)
    {
        var usedId = EffectContext.CurrentContext.NextImplicitId();
        var effectId = CreateEffectId(usedId);
        if (await effectResults.Contains(effectId))
            return false;
        
        var storedEffect = StoredEffect.CreateCompleted(effectId, alias: id);
        await effectResults.Set(storedEffect, flush: true);
        return true;
    }
    
    public Task<T> CreateOrGet<T>(string id, T value, bool flush = true) => CreateOrGet(CreateEffectId(id), value, flush);
    internal Task<T> CreateOrGet<T>(EffectId effectId, T value, bool flush) => effectResults.CreateOrGet(effectId, value, flush);

    public async Task Upsert<T>(string id, T value, bool flush = true)
    {
        // Try to resolve the id as either an effect ID or alias
        var resolvedEffectId = await effectResults.ResolveEffectIdOrAlias(id);
        if (resolvedEffectId != null)
        {
            await Upsert(resolvedEffectId, value, flush);
        }
        else
        {
            // If not found, create a new effect with the given ID
            await Upsert(CreateEffectId(id, EffectType.Effect), value, flush);
        }
    }

    internal Task Upsert<T>(EffectId effectId, T value, bool flush) => effectResults.Upsert(effectId, value, flush);

    internal Task Upserts(IEnumerable<Tuple<EffectId, object>> values, bool flush)
        => effectResults.Upserts(values, flush);

    public async Task<Option<T>> TryGet<T>(string id) => await TryGet<T>(CreateEffectId(id, EffectType.Effect));
    internal Task<Option<T>> TryGet<T>(EffectId effectId) => effectResults.TryGet<T>(effectId);
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
        => InnerCapture(
            id: EffectContext.CurrentContext.NextImplicitId(),
            alias: null,
            EffectType.Effect,
            work: () =>
            {
                work();
                return Task.CompletedTask;
            },
            resiliency,
            EffectContext.CurrentContext,
            retryPolicy: null
        );

    public Task<T> Capture<T>(Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => InnerCapture(
            id: EffectContext.CurrentContext.NextImplicitId(),
            alias: null,
            EffectType.Effect,
            work: () => work().ToTask(),
            resiliency,
            EffectContext.CurrentContext,
            retryPolicy: null
        );

    public Task Capture(Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => InnerCapture(
            id: EffectContext.CurrentContext.NextImplicitId(),
            alias: null,
            EffectType.Effect,
            work,
            resiliency,
            EffectContext.CurrentContext,
            retryPolicy: null
        );

    public Task<T> Capture<T>(Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => InnerCapture(
            id: EffectContext.CurrentContext.NextImplicitId(),
            alias: null,
            EffectType.Effect,
            work,
            resiliency,
            EffectContext.CurrentContext,
            retryPolicy: null
        );

    public Task Capture(Action work, RetryPolicy retryPolicy, bool flush = true)
        => InnerCapture(
            id: EffectContext.CurrentContext.NextImplicitId(),
            alias: null,
            EffectType.Effect,
            work: () =>
            {
                work();
                return Task.CompletedTask;
            },
            flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush,
            EffectContext.CurrentContext,
            retryPolicy
        );

    public Task<T> Capture<T>(Func<T> work, RetryPolicy retryPolicy, bool flush = true)
        => InnerCapture(
            id: EffectContext.CurrentContext.NextImplicitId(),
            alias: null,
            EffectType.Effect,
            work: () => work().ToTask(),
            flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush,
            EffectContext.CurrentContext,
            retryPolicy
        );

    public Task Capture(Func<Task> work, RetryPolicy retryPolicy, bool flush = true)
        => InnerCapture(
            id: EffectContext.CurrentContext.NextImplicitId(),
            alias: null,
            EffectType.Effect,
            work,
            flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush,
            EffectContext.CurrentContext,
            retryPolicy
        );

    public Task<T> Capture<T>(Func<Task<T>> work, RetryPolicy retryPolicy, bool flush = true)
        => InnerCapture(
            id: EffectContext.CurrentContext.NextImplicitId(),
            alias: null,
            EffectType.Effect,
            work,
            flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush,
            EffectContext.CurrentContext,
            retryPolicy
        );
    
    #endregion
    
    public Task Capture(string alias, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(alias, work: () => { work(); return Task.CompletedTask; }, resiliency);
    public Task<T> Capture<T>(string alias, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(alias, work: () => work().ToTask(), resiliency);
    public async Task Capture(string alias, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce) 
        => await InnerCapture(EffectContext.CurrentContext.NextImplicitId(), alias, EffectType.Effect, work, resiliency, EffectContext.CurrentContext, retryPolicy: null);
    public async Task<T> Capture<T>(string alias, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce) 
        => await InnerCapture(EffectContext.CurrentContext.NextImplicitId(), alias, EffectType.Effect, work, resiliency, EffectContext.CurrentContext, retryPolicy: null);
    
    public Task Capture(string alias, Action work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(alias, work: () => { work(); return Task.CompletedTask; }, retryPolicy, flush);
    public Task<T> Capture<T>(string alias, Func<T> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(alias, work: () => work().ToTask(), retryPolicy, flush);
    public async Task Capture(string alias, Func<Task> work, RetryPolicy retryPolicy, bool flush = true) 
        => await InnerCapture(EffectContext.CurrentContext.NextImplicitId(), alias, EffectType.Effect, work, flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush, EffectContext.CurrentContext, retryPolicy);
    public async Task<T> Capture<T>(string alias, Func<Task<T>> work, RetryPolicy retryPolicy, bool flush = true) 
        => await InnerCapture(EffectContext.CurrentContext.NextImplicitId(), alias, EffectType.Effect, work, flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush, EffectContext.CurrentContext, retryPolicy);

    private async Task InnerCapture(string id, string? alias, EffectType effectType, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext, RetryPolicy? retryPolicy)
    {
        if (retryPolicy != null && resiliency == ResiliencyLevel.AtMostOnce)
            throw new InvalidOperationException("Retry policy cannot be used with AtMostOnce resiliency");

        if (retryPolicy == null)
            await effectResults.InnerCapture(id, alias, effectType, work, resiliency, effectContext);
        else
            await effectResults.InnerCapture(
                id, 
                alias, 
                effectType,
                work: () => retryPolicy.Invoke(work, effect: this, utcNow, flowMinimumTimeout),
                resiliency,
                effectContext
            );
    }
    
    private async Task<T> InnerCapture<T>(string id, string? alias, EffectType effectType, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext, RetryPolicy? retryPolicy)
    {
        if (retryPolicy != null && resiliency == ResiliencyLevel.AtMostOnce)
            throw new InvalidOperationException("Retry policy cannot be used with AtMostOnce resiliency");
                
        if (retryPolicy == null)
            return await effectResults.InnerCapture(id, alias, effectType, work, resiliency, effectContext);

        return await effectResults.InnerCapture(
            id,
            alias,
            effectType,
            work: () => retryPolicy.Invoke(work, effect: this, utcNow, flowMinimumTimeout),
            resiliency,
            effectContext
        );
    }

    public async Task Clear(string id)
    {
        // Try to resolve the id as either an effect ID or alias
        var resolvedEffectId = await effectResults.ResolveEffectIdOrAlias(id);
        if (resolvedEffectId != null)
        {
            await effectResults.Clear(resolvedEffectId, flush: true);
        }
        else
        {
            // If not found, try to clear using the given ID directly
            await effectResults.Clear(CreateEffectId(id), flush: true);
        }
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
        => id.ToEffectId(type, context: EffectContext.CurrentContext.Parent?.Serialize().Value);

    public Task Flush() => effectResults.Flush();
}