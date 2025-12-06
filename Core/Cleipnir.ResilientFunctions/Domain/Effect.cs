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
    internal async Task<bool> Contains(int id) => await Contains(CreateEffectId(id));
    internal Task<bool> Contains(EffectId effectId) => effectResults.Contains(effectId);

    internal IEnumerable<EffectId> EffectIds => effectResults.EffectIds;
    internal FlowMinimumTimeout FlowMinimumTimeout => flowMinimumTimeout;

    internal async Task<WorkStatus?> GetStatus(int id)
    {
        var effectId = CreateEffectId(id);
        var storedEffect = await effectResults.GetOrValueDefault(effectId);
        return storedEffect?.WorkStatus;
    }

    internal async Task<bool> Mark()
    {
        var effectId = CreateEffectId(EffectContext.CurrentContext.NextImplicitId());
        if (await effectResults.Contains(effectId))
            return false;

        var storedEffect = StoredEffect.CreateCompleted(effectId, alias: null);
        await effectResults.Set(storedEffect, flush: true);
        return true;
    }

    internal async Task<T> CreateOrGet<T>(string alias, T value, bool flush = true)
    {
        var effectId = await effectResults.GetEffectId(alias)
            ?? CreateNextImplicitId();

        return await CreateOrGet(
            effectId,
            value,
            alias,
            flush
        );
    }

    internal Task<T> CreateOrGet<T>(EffectId effectId, T value, string? alias, bool flush) => effectResults.CreateOrGet(effectId, value, alias, flush);

    internal async Task Upsert<T>(string alias, T value, bool flush = true)
        => await Upsert(
            CreateNextImplicitId(),
            value,
            alias,
            flush
        );
    internal Task Upsert<T>(EffectId effectId, T value, string? alias, bool flush) => effectResults.Upsert(effectId, alias, value, flush);

    internal Task Upserts(IEnumerable<Tuple<EffectId, object, string?>> values, bool flush)
        => effectResults.Upserts(values, flush);

    internal async Task<Option<T>> TryGet<T>(string alias)
    {
        var effectId = await effectResults.GetEffectId(alias);
        if (effectId == null)
            return Option.CreateNoValue<T>();

        return await effectResults.TryGet<T>(effectId);
    } 
    
    internal Task<Option<T>> TryGet<T>(EffectId effectId) => effectResults.TryGet<T>(effectId);
    internal async Task<T> Get<T>(string alias) => await Get<T>(
        await effectResults.GetEffectId(alias) ?? throw new InvalidOperationException($"Unknown alias: '{alias}'")
    );
    internal async Task<T> Get<T>(EffectId effectId)
    {
        var option = await TryGet<T>(effectId);
        
        if (!option.HasValue)
            throw new InvalidOperationException($"No value exists for id: '{effectId}'");

        return option.Value;
    }

    #region Implicit ids

    public Task Capture(Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias: null, work, resiliency);
    public Task<T> Capture<T>(Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias: null, work: () => work().ToTask(), resiliency);
    public Task Capture(Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias: null, work, resiliency);
    public Task<T> Capture<T>(Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias: null, work, resiliency);

    public Task Capture(Action work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias: null, work, retryPolicy, flush);
    public Task<T> Capture<T>(Func<T> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias: null, work: () => work().ToTask(), retryPolicy, flush);
    public Task Capture(Func<Task> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias: null, work, retryPolicy, flush);
    public Task<T> Capture<T>(Func<Task<T>> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias: null, work, retryPolicy, flush);
    
    public Task Capture(string alias, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias, work, resiliency);
    public Task<T> Capture<T>(string alias, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias, work: () => work().ToTask(), resiliency);
    public Task Capture(string alias, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias, work, resiliency);
    public Task<T> Capture<T>(string alias, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias, work, resiliency);

    public Task Capture(string alias, Action work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias, work, retryPolicy, flush);
    public Task<T> Capture<T>(string alias, Func<T> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias, work: () => work().ToTask(), retryPolicy, flush);
    public Task Capture(string alias, Func<Task> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias, work, retryPolicy, flush);
    public Task<T> Capture<T>(string alias, Func<Task<T>> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id: EffectContext.CurrentContext.NextEffectId(), alias, work, retryPolicy, flush);

    #endregion
    
    private Task Capture(EffectId id, string? alias, Action work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, alias, work: () => { work(); return Task.CompletedTask; }, resiliency);
    private Task<T> Capture<T>(EffectId id, string? alias, Func<T> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => Capture(id, alias, work: () => work().ToTask(), resiliency);
    private async Task Capture(EffectId id, string? alias, Func<Task> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => await InnerCapture(id, alias, work, resiliency, EffectContext.CurrentContext, retryPolicy: null);
    private async Task<T> Capture<T>(EffectId id, string? alias, Func<Task<T>> work, ResiliencyLevel resiliency = ResiliencyLevel.AtLeastOnce)
        => await InnerCapture(id, alias, work, resiliency, EffectContext.CurrentContext, retryPolicy: null);

    private Task Capture(EffectId id, string? alias, Action work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id, alias, work: () => { work(); return Task.CompletedTask; }, retryPolicy, flush);
    private Task<T> Capture<T>(EffectId id, string? alias, Func<T> work, RetryPolicy retryPolicy, bool flush = true)
        => Capture(id, alias, work: () => work().ToTask(), retryPolicy, flush);
    private async Task Capture(EffectId id, string? alias, Func<Task> work, RetryPolicy retryPolicy, bool flush = true)
        => await InnerCapture(id, alias, work, flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush, EffectContext.CurrentContext, retryPolicy);
    private async Task<T> Capture<T>(EffectId id, string? alias, Func<Task<T>> work, RetryPolicy retryPolicy, bool flush = true)
        => await InnerCapture(id, alias, work, flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush, EffectContext.CurrentContext, retryPolicy);

    private async Task InnerCapture(EffectId id, string? alias, Func<Task> work, ResiliencyLevel resiliency, EffectContext effectContext, RetryPolicy? retryPolicy)
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

    private async Task<T> InnerCapture<T>(EffectId id, string? alias, Func<Task<T>> work, ResiliencyLevel resiliency, EffectContext effectContext, RetryPolicy? retryPolicy)
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

    public Task<T> WhenAny<T>(EffectId id, params Task<T>[] tasks)
        => Capture(id, alias: null, work: async () => await await Task.WhenAny(tasks));
    public Task<T[]> WhenAll<T>(EffectId id, params Task<T>[] tasks)
        => Capture(id, alias: null, work: () => Task.WhenAll(tasks));

    public Task<T> WhenAny<T>(params Task<T>[] tasks)
        => WhenAny(EffectContext.CurrentContext.NextEffectId(), tasks);
    public Task<T[]> WhenAll<T>(params Task<T>[] tasks)
        => WhenAll(EffectContext.CurrentContext.NextEffectId(), tasks);

    internal int TakeNextImplicitId() => EffectContext.CurrentContext.NextImplicitId();

    internal EffectId CreateEffectId(int id)
    {
        var parent = EffectContext.CurrentContext.Parent;
        if (parent == null)
            return id.ToEffectId();

        return new EffectId([..parent.Value, id]);
    }

    internal EffectId CreateNextImplicitId()
    {
        var id = EffectContext.CurrentContext.NextImplicitId();
        var parent = EffectContext.CurrentContext.Parent;
        if (parent == null)
            return id.ToEffectId();

        return new EffectId([..parent.Value, id]);
    }

    public Task Flush() => effectResults.Flush();
}