using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Queuing;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public class Workflow
{
    public FlowId FlowId { get; }
    internal StoredId StoredId { get; }
    public Effect Effect { get; }

    private readonly QueueManager _queueManager;
    private readonly UtcNow _utcNow;
    private MessageWriter MessageWriter { get; }


    internal Workflow(FlowId flowId, StoredId storedId, Effect effect, QueueManager queueManager, UtcNow utcNow, MessageWriter messageWriter)
    {
        FlowId = flowId;
        StoredId = storedId;
        Effect = effect;
        _queueManager = queueManager;
        _utcNow = utcNow;
        MessageWriter = messageWriter;
    }

    public Task Delay(TimeSpan @for, bool suspend = true, string? alias = null) => Delay(until: _utcNow() + @for, suspend, alias);
    public Task Delay(DateTime until, bool suspend = true, string? alias = null)
    {
        var timeoutId = Effect.CreateNextImplicitId();

        async Task Inner()
        {
            var expiry = (await Effect.CreateOrGet(timeoutId, until.ToUniversalTime().Ticks, alias, flush: false))
                .ToDateTime();
            
            var now = _utcNow();
            if (now > expiry)
                return;
            
            Effect.FlowTimeouts.AddTimeout(timeoutId, expiry);
            if (suspend)
                throw new SuspendInvocationException();

            //do in-memory wait
            var delay = expiry - now;
            await Task.Delay(delay);
            Effect.FlowTimeouts.RemoveTimeout(timeoutId);    
        }

        return Inner();
    }

    public Task<T> Message<T>() where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull<T>(this, effectId);
        return CreateAndPull();
    }

    public Task<T?> Message<T>(DateTime waitUntil) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T?> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull<T>(this, effectId, waitUntil);
        return CreateAndPull();
    }

    public Task<T?> Message<T>(TimeSpan waitFor) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T?> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull<T>(this, effectId, waitFor);
        return CreateAndPull();
    }

    public Task<T> Message<T>(Func<T, bool> filter) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull(this, effectId, filter);
        return CreateAndPull();
    }

    public Task<T?> Message<T>(Func<T, bool> filter, DateTime waitUntil) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T?> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull(this, effectId, waitUntil, filter);
        return CreateAndPull();
    }

    public Task<T?> Message<T>(Func<T, bool> filter, TimeSpan waitFor) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T?> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull<T>(this, effectId, waitFor, filter);
        return CreateAndPull();
    }

    public Task AppendMessage(object msg, string? idempotencyKey = null) => MessageWriter.AppendMessage(msg, idempotencyKey);

    public Task<T> Parallelle<T>(Func<Task<T>> work) => Effect.RunParallelle(work);

    public string ExecutionTree()
    {
        return $"{FlowId} ({StoredId}):" + Environment.NewLine + Effect.ExecutionTree();
    }

    public Task<DateTime> UtcNow(bool flush = true, string? alias = null) =>
        alias == null
            ? Effect.Capture(
                work: () => _utcNow(),
                resiliency: flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush
            )
            : Effect.Capture(
                work: () => _utcNow(),
                alias: alias,
                resiliency: flush ? ResiliencyLevel.AtLeastOnce : ResiliencyLevel.AtLeastOnceDelayFlush
            );
}