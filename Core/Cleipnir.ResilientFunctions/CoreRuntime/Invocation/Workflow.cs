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
    public Utilities Utilities { get; }
    public Correlations Correlations { get; }
    public Synchronization Synchronization { get; }
   
    private QueueManager _queueManager;
    private readonly UtcNow _utcNow;
    private MessageWriter MessageWriter { get; }


    public Workflow(FlowId flowId, StoredId storedId, Effect effect, Utilities utilities, Correlations correlations, DistributedSemaphores semaphores, QueueManager queueManager, UtcNow utcNow, MessageWriter messageWriter)
    {
        FlowId = flowId;
        StoredId = storedId;
        Utilities = utilities;
        Effect = effect;
        Correlations = correlations;
        Synchronization = new Synchronization(semaphores);
        _queueManager = queueManager;
        _utcNow = utcNow;
        MessageWriter = messageWriter;
    }

    public async Task RegisterCorrelation(string correlation) => await Correlations.Register(correlation);

    public Task Delay(TimeSpan @for, bool suspend = true, string? alias = null) => Delay(until: _utcNow() + @for, suspend, alias);
    public Task Delay(DateTime until, bool suspend = true, string? alias = null)
    {
        var effectId = Effect.TakeNextImplicitId();
        var timeoutId = EffectId.CreateWithCurrentContext(effectId);

        async Task Inner()
        {
            var expiry = await Effect.CreateOrGet(timeoutId, until.ToUniversalTime().Ticks, alias, flush: false);

            if (expiry == -1)
            {
                return;
            }

            Effect.FlowMinimumTimeout.AddTimeout(timeoutId, expiry.ToDateTime());
            var delay = (expiry.ToDateTime() - _utcNow()).RoundUpToZero();
            if (!suspend)
            {
                await Task.Delay(delay);
                delay = TimeSpan.Zero;
            }

            if (delay > TimeSpan.Zero)
                throw new SuspendInvocationException();
        
            await Effect.Upsert(timeoutId, -1L, alias, flush: false);
            Effect.FlowMinimumTimeout.RemoveTimeout(timeoutId);
        }

        return Inner();
    }

    public Task<T> Message<T>(TimeSpan? maxWait = null) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull<T>(this, effectId, filter: null, maxWait);
        return CreateAndPull();
    }

    public Task<T?> Message<T>(DateTime waitUntil, TimeSpan? maxWait = null) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T?> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull<T>(this, effectId, waitUntil, filter: null, maxWait);
        return CreateAndPull();
    }

    public Task<T?> Message<T>(TimeSpan waitFor, TimeSpan? maxWait = null) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T?> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull<T>(this, effectId, waitFor, filter: null, maxWait);
        return CreateAndPull();
    }

    public Task<T> Message<T>(Func<T, bool> filter, TimeSpan? maxWait = null) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull(this, effectId, filter, maxWait);
        return CreateAndPull();
    }

    public Task<T?> Message<T>(Func<T, bool> filter, DateTime waitUntil, TimeSpan? maxWait = null) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T?> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull(this, effectId, waitUntil, filter, maxWait);
        return CreateAndPull();
    }

    public Task<T?> Message<T>(Func<T, bool> filter, TimeSpan waitFor, TimeSpan? maxWait = null) where T : class
    {
        var effectId = Effect.CreateNextImplicitId();
        async Task<T?> CreateAndPull() => await (await _queueManager.CreateQueueClient()).Pull<T>(this, effectId, waitFor, filter, maxWait);
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