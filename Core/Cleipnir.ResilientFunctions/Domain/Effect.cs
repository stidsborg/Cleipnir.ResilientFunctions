using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Queuing;
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
    internal bool Contains(int id) => Contains(CreateEffectId(id));
    internal bool Contains(EffectId effectId) => effectResults.Contains(effectId);

    internal IEnumerable<EffectId> EffectIds => effectResults.EffectIds;
    internal FlowMinimumTimeout FlowMinimumTimeout => flowMinimumTimeout;

    internal WorkStatus? GetStatus(int id)
    {
        var effectId = CreateEffectId(id);
        var storedEffect = effectResults.GetOrValueDefault(effectId);
        return storedEffect?.WorkStatus;
    }

    internal async Task<bool> Mark(bool flush) => await Mark(EffectContext.CurrentContext.NextEffectId(), flush);
    internal async Task<bool> Mark(EffectId effectId, bool flush)
    {
        if (effectResults.Contains(effectId))
            return false;

        var storedEffect = StoredEffect.CreateCompleted(effectId, alias: null);
        await effectResults.Set(storedEffect, flush);
        return true;
    }

    internal async Task<T> CreateOrGet<T>(string alias, T value, bool flush = true)
    {
        var effectId = effectResults.GetEffectId(alias)
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

    internal Task Upserts(IEnumerable<EffectResult> values, bool flush)
        => effectResults.Upserts(values, flush);

    internal Option<T> TryGet<T>(string alias)
    {
        var effectId = effectResults.GetEffectId(alias);
        if (effectId == null)
            return Option.CreateNoValue<T>();

        return effectResults.TryGet<T>(effectId);
    }

    internal Option<T> TryGet<T>(EffectId effectId) => effectResults.TryGet<T>(effectId);
    internal Option<object?> TryGet(EffectId effectId, Type type) => effectResults.TryGet(effectId, type);
    internal T Get<T>(string alias) => Get<T>(
        effectResults.GetEffectId(alias) ?? throw new InvalidOperationException($"Unknown alias: '{alias}'")
    );
    internal T Get<T>(EffectId effectId)
    {
        var option = TryGet<T>(effectId);

        if (!option.HasValue)
            throw new InvalidOperationException($"No value exists for id: '{effectId}'");

        return option.Value;
    }
    
    internal IReadOnlyList<EffectId> GetChildren(EffectId effectId) => effectResults.GetChildren(effectId);

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

    public async Task ForEach<T>(IEnumerable<T> elms, Func<T, Task> handler, string? alias = null)
    {
        var id = EffectContext.CurrentContext.NextEffectId();
        var atIndex = await CreateOrGet(id, value: 0, alias, flush: false);
        
        foreach (var elm in elms.Skip(atIndex))
        {
            var prevParent = id.CreateChild(atIndex - 1);
            await effectResults.Clear(prevParent, flush: true);
            
            var parent = id.CreateChild(atIndex);
            await Capture(
                parent,
                alias: null,
                work: () => handler(elm)
            );
            
            atIndex++;
            await Upsert(id, atIndex, alias, flush: false);
        }
        
        var lastChild = id.CreateChild(atIndex - 1);
        await effectResults.Clear(lastChild, flush: false);
    }

    internal async Task Clear(EffectId id, bool flush) => await effectResults.Clear(id, flush);
    internal bool IsDirty(EffectId id) => effectResults.IsDirty(id);
    
    public async Task<TSeed> AggregateEach<T, TSeed>(
        IEnumerable<T> elms,
        TSeed seed,
        Func<T, TSeed, Task<TSeed>> handler, 
        string? alias = null)
    {
        var id = EffectContext.CurrentContext.NextEffectId();
        var atIndexAndAkk = await CreateOrGet(id, value: Tuple.Create(0, seed), alias, flush: false);
        var atIndex = atIndexAndAkk.Item1;
        var akk = atIndexAndAkk.Item2;
        
        foreach (var elm in elms.Skip(atIndex))
        {
            var prevParent = id.CreateChild(atIndex - 1);
            await effectResults.Clear(prevParent, flush: true);
            
            var parent = id.CreateChild(atIndex);
            akk = await Capture(
                parent,
                alias: null,
                work: () => handler(elm, akk)
            );
            
            atIndex++;
            await Upsert(id, Tuple.Create(atIndex, akk), alias, flush: false);
        }
        
        var lastChild = id.CreateChild(atIndex - 1);
        await effectResults.Clear(lastChild, flush: false);
        return akk;
    }

    internal void RegisterQueueManager(QueueManager queueManager) => effectResults.QueueManager = queueManager;

    internal string PrintEffects() => EffectPrinter.Print(effectResults);
}