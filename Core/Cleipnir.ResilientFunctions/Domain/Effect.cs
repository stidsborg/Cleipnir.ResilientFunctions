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
    public async Task<bool> Contains(int id) => await Contains(CreateEffectId(id));
    internal Task<bool> Contains(EffectId effectId) => effectResults.Contains(effectId);

    internal IEnumerable<EffectId> EffectIds => effectResults.EffectIds;
    internal FlowMinimumTimeout FlowMinimumTimeout => flowMinimumTimeout;

    public async Task<WorkStatus?> GetStatus(int id)
    {
        var effectId = CreateEffectId(id);
        var storedEffect = await effectResults.GetOrValueDefault(effectId);
        return storedEffect?.WorkStatus;
    }

    public async Task<bool> Mark(int id)
    {
        var effectId = CreateEffectId(id);
        if (await effectResults.Contains(effectId))
            return false;

        var storedEffect = StoredEffect.CreateCompleted(effectId, alias: id.ToString());
        await effectResults.Set(storedEffect, flush: true);
        return true;
    }

    public Task<T> CreateOrGet<T>(int id, T value, bool flush = true) => CreateOrGet(CreateEffectId(id), value, id.ToString(), flush);
    internal Task<T> CreateOrGet<T>(EffectId effectId, T value, string? alias, bool flush) => effectResults.CreateOrGet(effectId, value, alias, flush);

    public async Task Upsert<T>(int id, T value, bool flush = true) => await Upsert(CreateEffectId(id), value, id.ToString(), flush);
    internal Task Upsert<T>(EffectId effectId, T value, string? alias, bool flush) => effectResults.Upsert(effectId, alias, value, flush);

    internal Task Upserts(IEnumerable<Tuple<EffectId, object, string?>> values, bool flush)
        => effectResults.Upserts(values, flush);

    public async Task<Option<T>> TryGet<T>(int id) => await TryGet<T>(CreateEffectId(id));
    internal Task<Option<T>> TryGet<T>(EffectId effectId) => effectResults.TryGet<T>(effectId);
    public async Task<T> Get<T>(int id) => await Get<T>(CreateEffectId(id));
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

    public Task Capture(Action work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId(), work, retryPolicy, flush);
    public Task<T> Capture<T>(Func<T> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId(), work: () => work().ToTask(), retryPolicy, flush);
    public Task Capture(Func<Task> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId(), work, retryPolicy, flush);
    public Task<T> Capture<T>(Func<Task<T>> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextImplicitId(), work, retryPolicy, flush);

    #endregion
    
    private Task Capture(int id, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => { work(); return Task.CompletedTask; }, resiliency);
    private Task<T> Capture<T>(int id, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, work: () => work().ToTask(), resiliency);
    private async Task Capture(int id, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => await InnerCapture(id, alias: id.ToString(), work, resiliency, EffectContext.CurrentContext, retryPolicy: null);
    private async Task<T> Capture<T>(int id, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => await InnerCapture(id, alias: id.ToString(), work, resiliency, EffectContext.CurrentContext, retryPolicy: null);

    private Task Capture(int id, Action work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id, work: () => { work(); return Task.CompletedTask; }, retryPolicy, flush);
    private Task<T> Capture<T>(int id, Func<T> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id, work: () => work().ToTask(), retryPolicy, flush);
    private async Task Capture(int id, Func<Task> work, RetryPolicy retryPolicy, bool flush = true)
        => await InnerCapture(id, alias: id.ToString(), work, flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush, EffectContext.CurrentContext, retryPolicy);
    private async Task<T> Capture<T>(int id, Func<Task<T>> work, RetryPolicy retryPolicy, bool flush = true)
        => await InnerCapture(id, alias: id.ToString(), work, flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush, EffectContext.CurrentContext, retryPolicy);

    private async Task InnerCapture(int id, string? alias, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext, RetryPolicy? retryPolicy)
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

    private async Task<T> InnerCapture<T>(int id, string? alias, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext, RetryPolicy? retryPolicy)
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

    public Task Clear(int id) => effectResults.Clear(CreateEffectId(id), flush: true);

    public Task<T> WhenAny<T>(int id, params Task<T>[] tasks)
        => Capture(id, work: async () => await await Task.WhenAny(tasks));
    public Task<T[]> WhenAll<T>(int id, params Task<T>[] tasks)
        => Capture(id, work: () => Task.WhenAll(tasks));

    public Task<T> WhenAny<T>(params Task<T>[] tasks)
        => WhenAny(EffectContext.CurrentContext.NextImplicitId(), tasks);
    public Task<T[]> WhenAll<T>(params Task<T>[] tasks)
        => WhenAll(EffectContext.CurrentContext.NextImplicitId(), tasks);

    internal int TakeNextImplicitId() => EffectContext.CurrentContext.NextImplicitId();

    internal EffectId CreateEffectId(int id)
    {
        var parent = EffectContext.CurrentContext.Parent;
        if (parent == null)
            return id.ToEffectId();

        var parentContext = parent.Context;
        var newContext = new int[parentContext.Length + 1];
        System.Array.Copy(parentContext, newContext, parentContext.Length);
        newContext[^1] = parent.Id;
        return id.ToEffectId(newContext);
    }

    public Task Flush() => effectResults.Flush();
}