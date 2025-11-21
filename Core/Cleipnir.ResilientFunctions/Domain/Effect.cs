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
    public async Task<bool> Contains(string id) => await Contains(CreateEffectId(id));
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
        var effectId = CreateEffectId(id);
        if (await effectResults.Contains(effectId))
            return false;
        
        var storedEffect = StoredEffect.CreateCompleted(effectId, alias: id);
        await effectResults.Set(storedEffect, flush: true);
        return true;
    }
    
    public Task<T> CreateOrGet<T>(string id, T value, bool flush = true) => CreateOrGet(CreateEffectId(id), value, id, flush);
    internal Task<T> CreateOrGet<T>(EffectId effectId, T value, string? alias, bool flush) => effectResults.CreateOrGet(effectId, value, alias, flush);

    public async Task Upsert<T>(string id, T value, bool flush = true) => await Upsert(CreateEffectId(id), value, id, flush);
    internal Task Upsert<T>(EffectId effectId, T value, string? alias, bool flush) => effectResults.Upsert(effectId, alias, value, flush);

    internal Task Upserts(IEnumerable<Tuple<EffectId, object, string?>> values, bool flush)
        => effectResults.Upserts(values, flush);

    public async Task<Option<T>> TryGet<T>(string id) => await TryGet<T>(CreateEffectId(id));
    internal Task<Option<T>> TryGet<T>(EffectId effectId) => effectResults.TryGet<T>(effectId);
    public async Task<T> Get<T>(string id) => await Get<T>(CreateEffectId(id));
    internal async Task<T> Get<T>(EffectId effectId)
    {
        var option = await TryGet<T>(effectId);
        
        if (!option.HasValue)
            throw new InvalidOperationException($"No value exists for id: '{effectId}'");

        return option.Value;
    }

    #region Implicit ids

    public Task Capture(Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId().ToString(), work, resiliency);
    public Task<T> Capture<T>(Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId().ToString(), work: () => work().ToTask(), resiliency);
    public Task Capture(Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId().ToString(), work, resiliency);
    public Task<T> Capture<T>(Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId().ToString(), work, resiliency);

    public Task Capture(Action work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId().ToString(), work, retryPolicy, flush);
    public Task<T> Capture<T>(Func<T> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId().ToString(), work: () => work().ToTask(), retryPolicy, flush);
    public Task Capture(Func<Task> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId().ToString(), work, retryPolicy, flush);
    public Task<T> Capture<T>(Func<Task<T>> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId().ToString(), work, retryPolicy, flush);
    
    #endregion
    
    private Task Capture(string id, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => { work(); return Task.CompletedTask; }, resiliency);
    private Task<T> Capture<T>(string id, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => work().ToTask(), resiliency);
    private async Task Capture(string id, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => await InnerCapture(id, alias: id, work, resiliency, EffectContext.CurrentContext, retryPolicy: null);
    private async Task<T> Capture<T>(string id, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => await InnerCapture(id, alias: id, work, resiliency, EffectContext.CurrentContext, retryPolicy: null);
    
    private Task Capture(string id, Action work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id, work: () => { work(); return Task.CompletedTask; }, retryPolicy, flush);
    private Task<T> Capture<T>(string id, Func<T> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id, work: () => work().ToTask(), retryPolicy, flush);
    private async Task Capture(string id, Func<Task> work, RetryPolicy retryPolicy, bool flush = true)
        => await InnerCapture(id, alias: id, work, flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush, EffectContext.CurrentContext, retryPolicy);
    private async Task<T> Capture<T>(string id, Func<Task<T>> work, RetryPolicy retryPolicy, bool flush = true)
        => await InnerCapture(id, alias: id, work, flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush, EffectContext.CurrentContext, retryPolicy);

    private async Task InnerCapture(string id, string? alias, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext, RetryPolicy? retryPolicy)
    {
        if (retryPolicy != null && resiliency == ResiliencyLevel.AtMostOnce)
            throw new InvalidOperationException("Retry policy cannot be used with AtMostOnce resiliency");

        if (retryPolicy == null)
            await effectResults.InnerCapture(id, alias, work, resiliency, effectContext);
        else
            await effectResults.InnerCapture(
                id,
                alias,
                work: () => retryPolicy.Invoke(work, effect: this, utcNow, flowMinimumTimeout),
                resiliency,
                effectContext
            );
    }

    private async Task<T> InnerCapture<T>(string id, string? alias, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext, RetryPolicy? retryPolicy)
    {
        if (retryPolicy != null && resiliency == ResiliencyLevel.AtMostOnce)
            throw new InvalidOperationException("Retry policy cannot be used with AtMostOnce resiliency");

        if (retryPolicy == null)
            return await effectResults.InnerCapture(id, alias, work, resiliency, effectContext);

        return await effectResults.InnerCapture(
            id,
            alias,
            work: () => retryPolicy.Invoke(work, effect: this, utcNow, flowMinimumTimeout),
            resiliency,
            effectContext
        );
    }

    public Task Clear(string id) => effectResults.Clear(CreateEffectId(id), flush: true);
    
    public Task<T> WhenAny<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: async () => await await Task.WhenAny(tasks));
    public Task<T[]> WhenAll<T>(string id, params Task<T>[] tasks)
        => Capture(id, work: () => Task.WhenAll(tasks));

    public Task<T> WhenAny<T>(params Task<T>[] tasks)
        => WhenAny(EffectContext.CurrentContext.NextImplicitId().ToString(), tasks);
    public Task<T[]> WhenAll<T>(params Task<T>[] tasks)
        => WhenAll(EffectContext.CurrentContext.NextImplicitId().ToString(), tasks);

    internal string TakeNextImplicitId() => EffectContext.CurrentContext.NextImplicitId().ToString();

    internal EffectId CreateEffectId(string id)
        => id.ToEffectId(context: EffectContext.CurrentContext.Parent?.Serialize().Value);

    public Task Flush() => effectResults.Flush();
}